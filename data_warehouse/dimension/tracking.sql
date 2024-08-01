-- Alter Source Tables
-- Questionnaire
ALTER TABLE questionnaire
ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Create the Trigger Function
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the Trigger
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON questionnaire
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

-- Question Table
ALTER TABLE question
ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE question
ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE TRIGGER set_timestamp_question
BEFORE UPDATE ON question
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();


-- Organization Table
ALTER TABLE organization
ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE TRIGGER set_timestamp_organization
BEFORE UPDATE ON organization
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();
