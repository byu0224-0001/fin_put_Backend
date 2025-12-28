-- migration: add_hierarchical_sectors.sql
ALTER TABLE investor_sector
ADD COLUMN IF NOT EXISTS sector_l1 VARCHAR(50),
ADD COLUMN IF NOT EXISTS sector_l2 VARCHAR(50),
ADD COLUMN IF NOT EXISTS sector_l3_tags JSONB DEFAULT '[]'::jsonb,
ADD COLUMN IF NOT EXISTS confidence_l2 FLOAT,
ADD COLUMN IF NOT EXISTS confidence_l3 JSONB DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS business_model_role VARCHAR(50),
ADD COLUMN IF NOT EXISTS rule_version VARCHAR(20),
ADD COLUMN IF NOT EXISTS rule_confidence FLOAT,
ADD COLUMN IF NOT EXISTS training_label BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_investor_sector_l1 ON investor_sector(sector_l1);
CREATE INDEX IF NOT EXISTS idx_investor_sector_l2 ON investor_sector(sector_l2);

