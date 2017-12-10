--
-- Table structure for table `weibo`
--

DROP TABLE IF EXISTS `weibo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo` (
  `allow_all_act_msg` tinyint(1) DEFAULT NULL,
  `allow_all_comment` tinyint(1) DEFAULT NULL,
  `avatar_large` text,
  `bi_followers_count` bigint(20) DEFAULT NULL,
  `block_word` bigint(20) DEFAULT NULL,
  `city` bigint(20) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `description` text,
  `domain` text,
  `favourites_count` bigint(20) DEFAULT NULL,
  `follow_me` tinyint(1) DEFAULT NULL,
  `followers_count` bigint(20) DEFAULT NULL,
  `following` tinyint(1) DEFAULT NULL,
  `friends_count` bigint(20) DEFAULT NULL,
  `gender` text,
  `geo_enabled` tinyint(1) DEFAULT NULL,
  `id` bigint(20) DEFAULT NULL,
  `idstr` bigint(20) DEFAULT NULL,
  `lang` text,
  `location` text,
  `mbrank` bigint(20) DEFAULT NULL,
  `mbtype` bigint(20) DEFAULT NULL,
  `name` text,
  `online_status` bigint(20) DEFAULT NULL,
  `profile_image_url` text,
  `profile_url` bigint(20) DEFAULT NULL,
  `province` bigint(20) DEFAULT NULL,
  `remark` text,
  `screen_name` text,
  `star` bigint(20) DEFAULT NULL,
  `status_id` bigint(20) DEFAULT NULL,
  `statuses_count` bigint(20) DEFAULT NULL,
  `url` text,
  `verified` tinyint(1) DEFAULT NULL,
  `verified_reason` text,
  `verified_type` bigint(20) DEFAULT NULL,
  `weihao` bigint(20) DEFAULT NULL,
  `created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
