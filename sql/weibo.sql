-- MySQL dump 10.13  Distrib 5.6.33, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: pandas
-- ------------------------------------------------------
-- Server version	5.6.33-0ubuntu0.14.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

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
  `created_time` datetime DEFAULT NULL,
  `updated_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-12-06 13:44:11
