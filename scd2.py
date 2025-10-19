MERGE into target tgt
USING 
select id, id as merge_key, *, is_latest='y', is_deleted='n', md5(cdc cols)
from src
union all 
select id, null as merge_key, *, 'y' islatest, 'n' isdeleted, md5(cdc cols) from 
 src a join tgt b on a.id = b.id 
where ((a.key<>b.key) or b.is_deleted='Y') and b.is_latest='Y'
)

src

on tgt.id = src.mergekey
when matched and tgt.is_latest='y' and (tgt.md5 <> src.md5 or tgt.is_deleted='y') then update is_latest='n',updtts=currenttimestamp
when not matched then insert 
when not matched by source and is_latest='y' and is_deleted <>'y' then update set is_delete='y',lstupdt_ts=currenttimestamp

