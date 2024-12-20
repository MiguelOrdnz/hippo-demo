with avg_price_per_chain as (
    select
        chain,
        ndc,
        avg(quantity/price) as avg_price,
    from main.claims
        join main.pharmacies
            on pharmacies.npi = claims.npi
    group by
        chain,
        ndc,
)
select
    ndc,
    array_agg(struct_pack("name":= chain, "avg_price":= avg_price) ORDER BY avg_price ASC) as chain
from avg_price_per_chain
group by 
    ndc
