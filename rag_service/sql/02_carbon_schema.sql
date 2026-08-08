-- Carbon Knowledge Base
USE industrial_analytics;
CREATE TABLE IF NOT EXISTS carbon_knowledge
(
    knowledge_id UUID DEFAULT generateUUIDv4(),
    equipment_type String, -- e.g., 'Pump', 'Conveyor', 'HVAC'
    content String, -- Detailed information about carbon impact, efficiency standards, etc.
    embedding Array(Float32), -- Vector embedding of the content for semantic search
    metadata String DEFAULT '{}' -- JSON for source, region, standards, etc.
)
ENGINE = MergeTree
ORDER BY (equipment_type, knowledge_id);

-- Add carbon metrics to equipment tree
SET allow_experimental_inverted_index = 1;
ALTER TABLE equipment_tree
ADD COLUMN IF NOT EXISTS carbon_intensity_score Float32 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS efficiency_rating String DEFAULT '';
