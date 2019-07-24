Simple Kafka Streams project to check reproduce dying stream threads in real project.

Project contains also embedded kafka dependency which is not starting by default.
To run with embedded kafka just simply add environment variable "EMBEDDED_KAFKA" with any value (only null check is performed)