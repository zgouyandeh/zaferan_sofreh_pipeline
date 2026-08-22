
--databricks/02_transformation/04_fact_review.sql
--This code reads the unified silver customer reviews table, 
--applies additional transformations and quality checks, and writes the result to a fact_review table.
--It use the databricks SQL ai_query() function to call a large language model (LLM) for sentiment analysis 
--and issue classification.



-- Creates or streams updates to the Silver layer table for customer reviews
CREATE OR REFRESH STREAMING TABLE zaferan_sofreh.silver.fact_review (
    
    -- DLT DATA QUALITY EXPECTATIONS:
    -- Rows failing these validation checks will be dropped automatically (ON VIOLATION DROP ROW)
    -- to maintain high data quality in the Silver layer.
    
    CONSTRAINT valid_sentiment 
        EXPECT (sentiment IN ('positive', 'neutral', 'negative')) 
        ON VIOLATION DROP ROW,
        
    CONSTRAINT valid_delivery_severity 
        EXPECT (delivery_severity IN ('none', 'minor', 'moderate', 'severe')) 
        ON VIOLATION DROP ROW,
        
    CONSTRAINT valid_food_quality_severity 
        EXPECT (food_quality_severity IN ('none', 'minor', 'moderate', 'severe')) 
        ON VIOLATION DROP ROW,
        
    CONSTRAINT valid_pricing_severity 
        EXPECT (pricing_severity IN ('none', 'minor', 'moderate', 'severe')) 
        ON VIOLATION DROP ROW,
        
    CONSTRAINT valid_portion_size_severity 
        EXPECT (portion_size_severity IN ('none', 'minor', 'moderate', 'severe')) 
        ON VIOLATION DROP ROW,
        
    CONSTRAINT non_negative_rating 
        EXPECT (rating >= 0) 
        ON VIOLATION DROP ROW
)
AS
-- CTE: Calls Databricks AI Function to process customer reviews dynamically
WITH model_response AS (
    SELECT
        *,
        -- Invokes the LLM endpoint using Databricks SQL ai_query()
        ai_query(
            'databricks-gpt-oss-20b',
            
            -- PROMPT CONSTRUCTION:
            -- Forces the model to return ONLY a strictly formatted JSON response.
            CONCAT(
                'Analyze the following customer review and return ONLY a valid JSON object. ',
                'Do not include markdown, explanations, or any text outside the JSON object. ',
                'Use exactly this structure: ',
                '{',
                '"sentiment": "<positive/neutral/negative>", ',
                '"issue_delivery": <true/false>, ',
                '"delivery_severity": "<none/minor/moderate/severe>", ',
                '"issue_delivery_reason": "<brief reason or empty string>", ',
                '"issue_food_quality": <true/false>, ',
                '"food_quality_severity": "<none/minor/moderate/severe>", ',
                '"issue_food_quality_reason": "<brief reason or empty string>", ',
                '"issue_pricing": <true/false>, ',
                '"pricing_severity": "<none/minor/moderate/severe>", ',
                '"issue_pricing_reason": "<brief reason or empty string>", ',
                '"issue_portion_size": <true/false>, ',
                '"portion_size_severity": "<none/minor/moderate/severe>", ',
                '"issue_portion_size_reason": "<brief reason or empty string>"',
                '}. ',
                
                -- PROMPT RULES & CLASSIFICATION LOGIC:
                'Rules: ',
                '1. sentiment must be exactly one of: positive, neutral, negative. ',
                '2. Each issue field must be boolean: true or false. ',
                '3. If an issue is false, its severity MUST be "none" ',
                'and its reason MUST be an empty string. ',
                '4. If an issue is true, classify its severity as: ',
                'minor = small inconvenience or low-impact problem, ',
                'moderate = noticeable problem that negatively affects the experience, ',
                'severe = serious problem that significantly damages the experience. ',
                '5. The severity must reflect the actual severity described in the review, ',
                'not simply the overall sentiment of the review. ',
                '6. A positive review may still contain a minor or moderate issue. ',
                'Do not change the overall sentiment solely because an issue exists. ',
                '7. Delivery problems must ONLY be classified under delivery. ',
                'Do not classify delivery problems as food quality problems unless ',
                'the customer explicitly complains about the food quality itself. ',
                '8. Food quality refers to taste, freshness, temperature, cooking quality, ',
                'ingredients, preparation, or other characteristics of the food itself. ',
                '9. Pricing refers to complaints about price, cost, value for money, ',
                'or whether the food is worth the price. ',
                '10. Portion size refers specifically to the quantity or size of the food portion. ',
                '11. Keep each reason brief and directly supported by the review. ',
                
                -- Input text appended at the end of the prompt
                'Review text: ',
                review_text
            )
        ) AS analysis_json

    -- Streams new incoming records incrementally from the Bronze table
    FROM STREAM(zaferan_sofreh.bronze.customer_reviews_raw)
)

-- MAIN SELECTION: Extracts raw JSON values into typed SQL table columns
SELECT
    -- Base tracking identifiers
    review_id,
    order_id,
    customer_id,
    restaurant_id,
    rating,
    review_text,
    
    -- Preserve raw LLM output for auditing / debugging
    analysis_json,

    -- JSON PARSING SECTION:
    -- Extracts sentiment classification
    get_json_object(analysis_json, '$.sentiment') AS sentiment,

    -- Extracts Delivery Category attributes & casts flag to boolean
    get_json_object(analysis_json, '$.issue_delivery')::boolean AS issue_delivery,
    get_json_object(analysis_json, '$.delivery_severity') AS delivery_severity,
    get_json_object(analysis_json, '$.issue_delivery_reason') AS issue_delivery_reason,

    -- Extracts Food Quality Category attributes & casts flag to boolean
    get_json_object(analysis_json, '$.issue_food_quality')::boolean AS issue_food_quality,
    get_json_object(analysis_json, '$.food_quality_severity') AS food_quality_severity,
    get_json_object(analysis_json, '$.issue_food_quality_reason') AS issue_food_quality_reason,

    -- Extracts Pricing Category attributes & casts flag to boolean
    get_json_object(analysis_json, '$.issue_pricing')::boolean AS issue_pricing,
    get_json_object(analysis_json, '$.pricing_severity') AS pricing_severity,
    get_json_object(analysis_json, '$.issue_pricing_reason') AS issue_pricing_reason,

    -- Extracts Portion Size Category attributes & casts flag to boolean
    get_json_object(analysis_json, '$.issue_portion_size')::boolean AS issue_portion_size,
    get_json_object(analysis_json, '$.portion_size_severity') AS portion_size_severity,
    get_json_object(analysis_json, '$.issue_portion_size_reason') AS issue_portion_size_reason,

    review_timestamp

FROM model_response;