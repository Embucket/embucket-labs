{% docs zuora_order_source %}

In Zuora Billing, an [Order](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) represents a complete transaction record. Multiple order actions can be taken in a single order. For example, a subscription can be created and other subscriptions can be managed in a single order for a given customer.

{% enddocs %}

{% docs zuora_order_action_source %}

[Order Actions](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Order_Actions) are the tasks which can be performed on subscriptions in a single Order (see `dim_order` for more details). 

The following actions are supported in the Orders module:

- Create Subscription 
- Terms And Conditions
- Renewal
- Cancellation
- Owner Transfer
- Add Product
- Update Product
- Remove Product
- Suspend
- Resume

Multiple order actions can be grouped under a single Order. Previously multiple amendments would have been created to accomplish the same result. Now there can be a single `composite` amendment which encompasses all order actions taken in a single order.

{% enddocs %}