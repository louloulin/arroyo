-- Create a job that reads from the Push connector and writes to the console
CREATE TABLE push_source (
  message VARCHAR
) WITH (
  connector = 'push',
  topic = 'test-topic',
  protocol = 'http',
  format = 'json'
);

-- Create a job that reads from the Push connector and writes to the console
INSERT INTO print_sink
SELECT message FROM push_source;
