with src as(
    select *
    from {{source('DATN_RAW','BillOfMaterials')}}
)
select
    cast( _BillOfMaterialsID as string) as _id,
    ProductAssemblyID as product_id,
    ComponentID as component_id,
    BOMLevel,
    PerAssemblyQty,
    UnitMeasureCode,
    cast(StartDate as timestamp) as start_date,
    EndDate as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src