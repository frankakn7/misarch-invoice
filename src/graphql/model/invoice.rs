use async_graphql::{Error, Result, SimpleObject};
use bson::{doc, DateTime, Uuid};
use log::error;
use mongodb::{options::FindOneOptions, Collection};
use serde::{Deserialize, Serialize};

use crate::event::http_event_service::{HttpEventServiceState, OrderEventData};

use super::{
    super::query::query_object,
    foreign_types::{User, UserAddress, VendorAddress},
};

static INVOICE_TERMS: &str = "This invoice is created according the the companies terms and conditions specified on the website.";

/// Invoice of an order.
#[derive(Debug, Serialize, Deserialize, SimpleObject, Clone)]
pub struct Invoice {
    pub _id: Uuid,
    pub order_id: Uuid,
    pub issued_at: DateTime,
    pub content: String,
    pub user_address: UserAddress,
    pub vendor_address: VendorAddress,
    pub vat_number: Option<String>,
}

impl Invoice {
    /// Creates a new invoice from `OrderEventData` and `HttpEventServiceState` (containing the database connections).
    pub async fn new(
        order_event_data: OrderEventData,
        state: &HttpEventServiceState,
    ) -> Result<Self, Error> {
        let _id = Uuid::new();
        let (
            issued_at,
            issued_at_string,
            order_item_invoice_overview,
            user_address,
            vendor_address,
            user,
        ) = invoice_attribute_setup(&order_event_data, state).await?;
        let vat_number = order_event_data
            .vat_number
            .clone()
            .unwrap_or("-".to_string());
        let content = format!(
            r#"
# Invoice

### Company information:
{}
{}, {}
{}, {}

VAT number: {}

### Customer information:
ID: {}
Name: {}, {}
Address:
{}
{}, {}
{}, {}

### Invoice ID: {}, issued at: {} 

Terms and conditions: {}

---

Purchased items overview:

{}

---

Total compensatable amount: {}
"#,
            vendor_address.company_name,
            vendor_address.street1,
            vendor_address.street2,
            vendor_address.city,
            vendor_address.country,
            vat_number,
            user._id,
            user.first_name,
            user.last_name,
            user_address.company_name,
            user_address.street1,
            user_address.street2,
            user_address.city,
            user_address.country,
            _id,
            issued_at_string,
            INVOICE_TERMS,
            order_item_invoice_overview,
            order_event_data.compensatable_order_amount
        );
        let invoice = Invoice {
            _id,
            order_id: order_event_data.id,
            issued_at,
            content: content,
            user_address,
            vendor_address,
            vat_number: order_event_data.vat_number,
        };
        Ok(invoice)
    }
}

/// Sets up all the attributes from `OrderEventData` and `HttpEventServiceState` (containing the database connections) that are required for invoice creation.
async fn invoice_attribute_setup(
    order_event_data: &OrderEventData,
    state: &HttpEventServiceState,
) -> Result<(DateTime, String, String, UserAddress, VendorAddress, User), Error> {
    let issued_at = DateTime::now();
    let issued_at_string = issued_at
        .to_chrono()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    let order_item_invoice_overview = build_order_item_invoice_content(order_event_data);
    let user_address_user =
        query_user_address_user(&state.user_collection, order_event_data.invoice_address_id)
            .await
            .map_err(|e| { error!("step 1 query_user_address_user: {:?}", e); e })?;
    let user_address = project_user_to_user_address(user_address_user)
        .map_err(|e| { error!("step 2 project_user_to_user_address: {:?}", e); e })?;
    let vendor_address = query_vendor_address(&state.vendor_address_collection)
        .await
        .map_err(|e| { error!("step 3 query_vendor_address: {:?}", e); e })?;
    let user = query_object(&state.user_collection, order_event_data.user_id)
        .await
        .map_err(|e| { error!("step 4 query_object user: {:?}", e); e })?;
    Ok((
        issued_at,
        issued_at_string,
        order_item_invoice_overview,
        user_address,
        vendor_address,
        user,
    ))
}

/// Builds the part of the invoice content which describes the order items as a markdown table.
fn build_order_item_invoice_content(value: &OrderEventData) -> String {
    let mut content = String::new();
    content.push_str("| Item UUID | Product variant UUID | count | Compensatable amount |\n");
    content.push_str("| --- | --- | --- | --- |\n");
    for item in &value.order_items {
        content.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            item.id, item.product_variant_id, item.count, item.compensatable_amount
        ));
    }
    content
}

/// Shared function to query an address from a MongoDB collection of users.
/// Returns User which only contains the queried address.
pub async fn query_user_address_user(
    collection: &mongodb::Collection<User>,
    address_id: Uuid,
) -> Result<User> {
    let find_options = FindOneOptions::builder()
        .projection(Some(doc! {
            "addresses.$": 1,
            "_id": 1,
            "first_name": 1,
            "last_name": 1,
        }))
        .build();
    let message = format!("Address of UUID: `{}` not found.", address_id);
    match collection
        .find_one(
            doc! {"addresses": {
                "$elemMatch": {
                    "_id": address_id
                }
            }},
            Some(find_options),
        )
        .await
    {
        Ok(maybe_user) => maybe_user.ok_or(Error::new(message.clone())),
        Err(e) => Err(e.into()),
    }
}

/// Projects result of user address query, which is of type `User`, to the contained user address.
pub fn project_user_to_user_address(user: User) -> Result<UserAddress> {
    let message = format!("Projection failed, address could not be extracted from user.");
    user.addresses
        .iter()
        .next()
        .cloned()
        .ok_or(Error::new(message.clone()))
}

/// Shared function to query the current vendor address.
pub async fn query_vendor_address(collection: &Collection<VendorAddress>) -> Result<VendorAddress> {
    collection
        .find_one(None, None)
        .await?
        .ok_or(Error::new("Vendor address is not set locally."))
}
