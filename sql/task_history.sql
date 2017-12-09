DROP TABLE IF EXISTS task_history;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE task_history (
  task_id BIGINT NOT NULL,
  user_id VARCHAR(64) NOT NULL,
  secret_key VARCHAR(64) NOT NULL,
  status TINYINT NOT NULL DEFAULT 0,
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
