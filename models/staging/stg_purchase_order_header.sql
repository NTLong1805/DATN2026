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
    cast(TaxAmt as float64) as tax_amount,
    cast(Freight as float64) as ship_amount,
    cast(SubTotal as float64) as purchase_amount,
    cast(TotalDue as float64) as total_amount -- tax + ship + sub
from src