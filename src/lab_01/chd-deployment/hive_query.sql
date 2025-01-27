-- Complex query to analyze loan performance and customer behavior
WITH CustomerLoanSummary AS (
    -- Summarize loan applications per customer
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.life_event,
        COUNT(la.application_id) as total_applications,
        SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) as approved_applications,
        SUM(la.loan_amount) as total_loan_amount,
        SUM(la.marketing_cost) as total_marketing_cost,
        AVG(la.loan_amount) as avg_loan_amount
    FROM customers c
    LEFT JOIN loan_applications la ON c.customer_id = la.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.life_event
),
RepaymentMetrics AS (
    -- Calculate repayment performance metrics
    SELECT 
        la.customer_id,
        COUNT(DISTINCT lr.repayment_id) as total_repayments,
        SUM(lr.amount_paid) as total_amount_paid,
        AVG(lr.days_past_due) as avg_days_past_due,
        SUM(CASE WHEN lr.payment_status = 'LATE' THEN 1 ELSE 0 END) as late_payments
    FROM loan_applications la
    JOIN loan_repayments lr ON la.application_id = lr.loan_id
    GROUP BY la.customer_id
)

SELECT 
    cls.*,
    rm.total_repayments,
    rm.total_amount_paid,
    rm.avg_days_past_due,
    rm.late_payments,
    ROUND(CASE 
        WHEN cls.total_loan_amount = 0 THEN NULL 
        ELSE (cls.total_marketing_cost / cls.total_loan_amount) * 100 
    END, 2) as marketing_cost_percentage,
    ROUND(CASE 
        WHEN rm.total_repayments = 0 THEN NULL 
        ELSE (rm.late_payments / rm.total_repayments) * 100 
    END, 2) as late_payment_percentage,
    CASE 
        WHEN rm.avg_days_past_due = 0 THEN 'Excellent'
        WHEN rm.avg_days_past_due <= 30 THEN 'Good'
        WHEN rm.avg_days_past_due <= 90 THEN 'Fair'
        ELSE 'Poor'
    END as customer_rating
FROM CustomerLoanSummary cls
LEFT JOIN RepaymentMetrics rm ON cls.customer_id = rm.customer_id
WHERE cls.total_applications > 0
ORDER BY cls.total_loan_amount DESC, rm.avg_days_past_due ASC;
