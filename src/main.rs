#[macro_use]
extern crate crossbeam_channel;

use crossbeam_channel::unbounded;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

fn main() {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CustomerId(Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct OrderId(Uuid);

/// Messages sent from the "basket" service,
/// to the "order" service.
enum OrderRequest {
    /// A customer is attempting to make a new order.
    NewOrder(CustomerId),
    /// We're shutting down.
    ShutDown,
}

/// The result of making a new order,
/// sent by the "order" service, to the "basket" service.
struct OrderResult(CustomerId, bool);

enum CustomerType {
    Existing,
    New,
}

/// Messages sent from the "payment" service,
/// to the "fraud" service.
enum FraudCheck {
    /// Check an order for fraud.
    CheckOrder(OrderId),
    /// We're shutting down.
    ShutDown,
}

/// The result of making a fraud check,
/// sent by the "fraud" service, to the "payment" service.
struct FraudCheckResult(OrderId, bool);

/// Messages sent from the "order" service,
/// to the "payment" service.
enum PaymentRequest {
    /// Attempt a payment for an order,
    /// with a flag indicating whether it's a new customer.
    NewOrder(OrderId, CustomerType),
    /// We're shutting down.
    ShutDown,
}

/// The result of making a new payment,
/// sent by the "payment" service, to the "order" service.
struct PaymentResult(OrderId, bool);

#[test]
fn single_writer() {
    let (order_result_sender, order_result_receiver) = unbounded();
    let (payment_request_sender, payment_request_receiver) = unbounded();
    let (payment_result_sender, payment_result_receiver) = unbounded();
    let (fraud_check_sender, fraud_check_receiver) = unbounded();
    let (fraud_check_result_sender, fraud_check_result_receiver) = unbounded();
    let (order_request_sender, order_request_receiver) = unbounded();

    // A map of orders pending payment, owned by the "order" service.
    let mut pending_payment = HashMap::new();
    let mut existing_customers = HashSet::new();

    // Spawn the "order" service.
    let _ = thread::spawn(move || loop {
        select! {
            recv(order_request_receiver) -> msg => {
                match msg {
                    Ok(OrderRequest::NewOrder(customer_id)) => {
                        let order_id = OrderId(Uuid::new_v4());

                        let msg = if existing_customers.contains(&customer_id) {
                            PaymentRequest::NewOrder(order_id, CustomerType::Existing)
                        } else {
                            existing_customers.insert(customer_id.clone());
                            PaymentRequest::NewOrder(order_id, CustomerType::New)
                        };

                        let _ = payment_request_sender.send(msg);

                        pending_payment.insert(order_id.clone(), customer_id);
                    }
                    Ok(OrderRequest::ShutDown) => {
                        assert!(pending_payment.is_empty());
                        let _ = payment_request_sender.send(PaymentRequest::ShutDown);
                        break;
                    },
                    Err(_) => panic!("Error receiving an order request."),
                }
            },
            recv(payment_result_receiver) -> msg => {
                match msg {
                    Ok(PaymentResult(id, succeeded)) => {
                        let customer_id = pending_payment.remove(&id).expect("Payment result received for unknown order.");
                        existing_customers.insert(customer_id.clone());
                        let _ = order_result_sender.send(OrderResult(customer_id, succeeded));
                    }
                    _ => panic!("Error receiving a payment result."),
                }
            },
        }
    });

    fn check_for_fraud(_order: &OrderId) -> bool {
        thread::sleep(Duration::from_millis(100));
        true
    }

    // Spawn the "fraud" service.
    let _ = thread::spawn(move || loop {
        match fraud_check_receiver.recv() {
            Ok(FraudCheck::CheckOrder(order_id)) => {
                // First, check for fraud.
                let result = check_for_fraud(&order_id);

                // Send the result.
                let _ = fraud_check_result_sender.send(FraudCheckResult(order_id, result));
            }
            Ok(FraudCheck::ShutDown) => {
                break;
            }
            Err(_) => panic!("Error receiving a fraud check."),
        }
    });

    // A set of orders from first customer,
    // that are awaiting a fraud check.
    let mut awaiting_fraud_check = HashSet::new();

    // Spawn the "payment" service.
    let _ = thread::spawn(move || loop {
        select! {
            recv(payment_request_receiver) -> msg => {
                match msg {
                    Ok(PaymentRequest::NewOrder(order_id, customer_type)) => {
                        match customer_type {
                            CustomerType::Existing => {
                                // Process the payment for a new order without checking.
                                let _ = payment_result_sender.send(PaymentResult(order_id, true));
                            }
                            CustomerType::New => {
                                // Check for fraud.
                                awaiting_fraud_check.insert(order_id.clone());
                                let msg = FraudCheck::CheckOrder(order_id);
                                let _ = fraud_check_sender.send(msg);
                            }
                        }
                    }
                    Ok(PaymentRequest::ShutDown) => {
                        let _ = fraud_check_sender.send(FraudCheck::ShutDown);
                        break;
                    }
                    Err(_) => panic!("Error receiving a payment request."),
                }
            },
            recv(fraud_check_result_receiver) -> msg => {
                match msg {
                    Ok(FraudCheckResult(order_id, result)) => {
                        assert!(awaiting_fraud_check.remove(&order_id));

                        // Send the result.
                        let _ = payment_result_sender.send(PaymentResult(order_id, result));
                    },
                    Err(_) => panic!("Error receiving a fraud check result."),
                }
            },
        }
    });

    // A map of keeping count of pending order per customer,
    // owned by the "basket" service.
    let mut pending_orders = HashMap::new();

    // Create four customers.
    for _ in 0..4 {
        let customer_id = CustomerId(Uuid::new_v4());

        // Send two orders per customer.
        for _ in 0..2 {
            match pending_orders.entry(customer_id) {
                Entry::Vacant(entry) => {
                    entry.insert(1);
                }
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() += 1;
                }
            };
            let _ = order_request_sender.send(OrderRequest::NewOrder(customer_id));
        }
    }

    // Have the main thread of the test double-up as the "basket" service.
    loop {
        match order_result_receiver.recv() {
            Ok(OrderResult(customer_id, _succeeded)) => {
                match pending_orders.entry(customer_id) {
                    Entry::Vacant(_) => {
                        panic!("Got an order confirmation for an unknonw customer.");
                    }
                    Entry::Occupied(mut entry) => {
                        if *entry.get() == 1 {
                            entry.remove_entry();
                        } else {
                            *entry.get_mut() -= 1;
                        }
                    }
                };
                if pending_orders.is_empty() {
                    let _ = order_request_sender.send(OrderRequest::ShutDown);
                    break;
                }
            }
            _ => panic!("Error receiving an order result."),
        }
    }
}
