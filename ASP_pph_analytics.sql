select
asp.organization_id,
asp.mostRecentEnrollmentDate,
pph_a.saas,
pph_a.featureSum,
pph_a.plan_tier
from 
(SELECT
	organization_id
	,max(created_at) as mostRecentEnrollmentDate
FROM payment_plan_histories_analytics
WHERE enrollment=1
GROUP BY organization_id) asp
left join payment_plan_histories_analytics pph_a on pph_a.organization_id=asp.organization_id and pph_a.created_at=asp.mostRecentEnrollmentDate
where enrollment=1
group by asp.organization_id