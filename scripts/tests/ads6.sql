
select page_tab
from hive.tpch.multi_stage_ad_request_inspection
where page_tab in ('m_wilde_iphone', 'web') and userid > 1000000000;

;
