with src as(
    select *
    from {{source('DATN_RAW','ShoppingCartItem')}}
)
select
    cast(_ShoppingCartItemID as string) as shoppingcart_item_id,
    cast(ShoppingCartID as string) as _id,
    Quantity as quantity,
    cast(ProductID as string) as product_id,
    cast(DateCreated as timestamp) as date_created,
    cast(ModifiedDate as timestamp) as _ts
from src