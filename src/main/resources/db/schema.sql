CREATE TABLE lognote(
    note_id serial PRIMARY KEY,
    message varchar(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);