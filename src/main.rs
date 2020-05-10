#[macro_use]
extern crate crossbeam_channel;

use crossbeam_channel::unbounded;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::thread;
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

/// Messages sent from the "order" service,
/// to the "payment" service.
enum PaymentRequest {
    /// Attempt a payment for an order.
    NewOrder(OrderId),
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
    let (order_request_sender, order_request_receiver) = unbounded();

    // A map of orders pending payment, owned by the "order" service.
    let mut pending_payment = HashMap::new();

    // Spawn the "order" service.
    let _ = thread::spawn(move || loop {
        {
            select! {
                recv(order_request_receiver) -> msg => {
                    match msg {
                        Ok(OrderRequest::NewOrder(customer_id)) => {
                            let order_id = OrderId(Uuid::new_v4());
                            pending_payment.insert(order_id.clone(), customer_id);
                            let _ = payment_request_sender.send(PaymentRequest::NewOrder(order_id));
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
                            let _ = order_result_sender.send(OrderResult(customer_id, succeeded));
                        }
                        _ => panic!("Error receiving a payment result."),
                    }
                },
            }
        }
    });

    // Spawn the "payment" service.
    let _ = thread::spawn(move || loop {
        match payment_request_receiver.recv() {
            Ok(PaymentRequest::NewOrder(order_id)) => {
                // Process the payment for a new order.
                let _ = payment_result_sender.send(PaymentResult(order_id, true));
            }
            Ok(PaymentRequest::ShutDown) => {
                break;
            }
            Err(_) => panic!("Error receiving a payment request."),
        }
    });

    // A map of keeping count of pending order per customer,
    // owned by the "basket" service.
    let mut pending_orders = HashMap::new();

    for _ in 0..4 {
        let customer_id = CustomerId(Uuid::new_v4());
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
