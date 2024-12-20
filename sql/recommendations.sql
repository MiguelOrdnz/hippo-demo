with avg_price_per_chain as (
    select
        npi,
        ndc,
        avg(quantity/price) as avg_price,
    from main.claims
    group by
        npi,
        ndc,
)
select
    _metrics.ndc,
    array_agg(struct_pack("name":= chain, "avg_price":= _metrics.avg_price) ORDER BY avg_price ASC) as chain
from main.pharmacies
    left join avg_price_per_chain as _metrics
        on pharmacies.npi = _metrics.npi
group by 
    ndc