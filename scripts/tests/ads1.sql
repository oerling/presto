
select count(*)
from hive.tpch.multi_stage_ad_request_inspection
WHERE
    page_tab IN ('m_wilde_iphone', 'm_native_fb4a')
    AND ds = '1'
;

