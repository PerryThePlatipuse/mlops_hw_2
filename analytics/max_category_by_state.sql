SELECT us_state, argMaxMerge(category_state) AS cat_id
FROM max_category_by_state
GROUP BY us_state
ORDER BY us_state;
