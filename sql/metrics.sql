select
    npi,
    ndc,
    count(distinct claims.id) fills,
    count(distinct reverts.id) reverted,
    avg(claims.price) avg_price,
    sum(claims.price) total_price,
from main.claims
    left join main.reverts
        on claims.id = reverts.claim_id
group by 
    npi,
    ndc,