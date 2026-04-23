with src as(
    select *
    from {{source('DATN_RAW','SalesOrderHeader')}}
)
select
    cast(_SalesOrderID as string) as _id,
    cast(SalesOrderNumber as string) as order_number,
    RevisionNumber as revision_number,
    cast(OrderDate as timestamp) as order_date,
    cast(DueDate as timestamp) as due_date,
    cast(ShipDate as timestamp) as ship_date,
    Status as status,
    OnlineOrderFlag as is_online, -- KH Mua hàng onl hay off
    AccountNumber as account_number,
    PurchaseOrderNumber as purchase_order_number,
    cast(CustomerID as string) as customer_id,
    cast(SalesPersonID as string) as salesperson_id,
    cast(TerritoryID as string) as territory_id,
    cast(BillToAddressID as string) as bill_address_id, -- dia diem hoa don
    cast(ShipToAddressID as string) as ship_address_id, -- dia diem giao hang
    cast(ShipMethodID as string) as shipmethod_id,
    cast(CreditCardID as string) as creditcard_id,
    CreditCardApprovalCode as creditcard_approval,
    cast(CurrencyRateID as string) as currency_rate_id,
    cast(SubTotal as float64) as sales_amount,
    cast(TaxAmt as float64) as tax_amount,
    cast(Freight as float64) as ship_amount,
    cast(TotalDue as float64) as total_amount,
    Comment as comment,
    cast(ModifiedDate as timestamp) as _ts
from src