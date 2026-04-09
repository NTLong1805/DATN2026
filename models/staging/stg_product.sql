with src as(
    select *
    from {{source('DATN_RAW','Product')}}
)
select
    cast(_ProductID as string) as _id,
    ProductNumber as product_number,
    ProductModelID as product_model_id,
    cast(ProductSubcategoryID as string) as product_subcategory_id,
    Name as product_name,
    Size as size,
    Class as class,
    Color as color,
    Style as style,
    Weight as weight,
    MakeFlag as is_manufactured, -- 	0 = Product is purchased, 1 = Product is manufactured in-house.
    ListPrice as price,
    StandardCost as cost,
    ProductLine as type, --	R = Road, M = Mountain, T = Touring, S = Standard
    cast(SellStartDate as timestamp) as start_date,
    SellEndDate as end_date,
    DiscontinuedDate as end_manufacted_date,
    SafetyStockLevel as min_inventory_quantity,
    ReorderPoint as reorder_point_quantity,
    FinishedGoodsFlag as status,
    DaysToManufacture as manufacture_days
from src