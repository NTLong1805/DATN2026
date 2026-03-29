with src as(
    select *
    from {{source('DATN_RAW','PurchaseOrderHeader')}}
)
select
    cast(_PurchaseOrderID as string) as _id,
    RevisionNumber as revision_number,
    Status as status,
    cast(EmployeeID as string) as employee_id,
    cast(VendorID as string) as vendor_id,
    cast(ShipMethodID as string) as ship_method_id,
    cast(OrderDate as timestamp) as order_date,
    cast(ShipDate as timestamp) as ship_date,
    TaxAmt as tax_amount,
    Freight as ship_amount,
    SubTotal as purchase_amount,
    TotalDue as total_amount -- tax + ship + sub
from src