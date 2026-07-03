use async_graphql::{Enum, SimpleObject};
use bson::Uuid;
use serde::{Deserialize, Serialize};

use super::invoice::Invoice;

/// Foreign type of an order.
#[derive(Debug, Serialize, Deserialize, SimpleObject, Clone)]
pub struct Order {
    /// UUID of the order.
    pub _id: Uuid,
    /// Invoice of the order.
    pub invoice: Invoice,
}

/// Describes if order is placed, or yet pending. An order can be rejected during its lifetime.
#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Pending,
    Placed,
    Rejected,
}

/// Describes the reason why an order was rejected, in case of rejection: `OrderStatus::Rejected`.
#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RejectionReason {
    InvalidOrderData,
    InventoryReservationFailed,
}
