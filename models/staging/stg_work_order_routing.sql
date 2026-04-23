with src as(
    select *
    from {{source('DATN_RAW','WorkOrderRouting')}}
)
select
    cast(_WorkOrderID as string) as _id,
    cast(ProductID as string) as product_id,
    cast(OperationSequence as string) as operation_sequence,
    cast(LocationID as string) as location_id,
    cast(ScheduledStartDate as timestamp) as schedule_start_date,
    cast(ScheduledEndDate as timestamp) as schedule_end_date,
    safe_cast(ActualStartDate as timestamp) as real_start_date,
    safe_cast(ActualEndDate as timestamp) as real_end_date,
    cast(ActualResourceHrs as float64) as time_spent,
    cast(PlannedCost as float64) as planned_cost,
    cast(ActualCost as float64) as real_cost
from src