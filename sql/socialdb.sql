-- MySQL dump 10.13  Distrib 5.7.20, for Linux (x86_64)
--
-- Host: localhost    Database: pandas
-- ------------------------------------------------------
-- Server version	5.7.20-0ubuntu0.16.04.1

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
-- Table structure for table `task_history`
--

DROP TABLE IF EXISTS `task_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_history` (
  `uuid` varchar(64) NOT NULL,
  `task_id` bigint(20) NOT NULL,
  `user_id` varchar(64) NOT NULL,
  `secret_key` varchar(64) NOT NULL,
  `q` varchar(128) DEFAULT NULL,
  `ids` varchar(128) DEFAULT NULL,
  `province` varchar(4) DEFAULT NULL,
  `city` varchar(4) DEFAULT NULL,
  `starttime` bigint(20) DEFAULT NULL,
  `endtime` bigint(20) DEFAULT NULL,
  `hasv` varchar(4) DEFAULT NULL,
  `onlynum` int(4) DEFAULT '100',
  `status` tinyint(4) NOT NULL DEFAULT '0',
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `task_id_UNIQUE` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `weibo_comment`
--

DROP TABLE IF EXISTS `weibo_comment`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo_comment` (
  `id` bigint(20) NOT NULL,
  `uid` bigint(20) NOT NULL,
  `pid` bigint(20) NOT NULL,
  `text` text CHARACTER SET utf8mb4,
  `source` varchar(128) CHARACTER SET utf8mb4 NOT NULL,
  `mid` bigint(20) NOT NULL,
  `idstr` varchar(128) DEFAULT NULL,
  `floor_number` int(11) DEFAULT NULL,
  `disable_reply` tinyint(1) DEFAULT NULL,
  `reply_comment` text CHARACTER SET utf8mb4,
  `reply_original_text` text CHARACTER SET utf8mb4,
  `rootid` bigint(20) DEFAULT NULL,
  `source_allowclick` tinyint(1) DEFAULT NULL,
  `source_type` text,
  `apiState` text,
  `liveBizCode` text,
  `reply_like` text CHARACTER SET utf8mb4,
  `yellow_pic` text,
  `created_at` varchar(128) NOT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `mid` (`mid`),
  KEY `INX_WEIBO_COMMENT_CREATED_AT` (`created_at`),
  KEY `weibo_comment_user_info_idx` (`uid`),
  KEY `weibo_comment_post_status_idx` (`pid`),
  CONSTRAINT `weibo_comment_post_status` FOREIGN KEY (`pid`) REFERENCES `weibo_post_status` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `weibo_comment_user_info` FOREIGN KEY (`uid`) REFERENCES `weibo_user_info` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `weibo_media`
--

DROP TABLE IF EXISTS `weibo_media`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo_media` (
  `uuid` varchar(36) NOT NULL,
  `pid` bigint(20) NOT NULL,
  `url_ori` varchar(128) NOT NULL,
  `super_topic_status_count` int(11) DEFAULT NULL,
  `super_topic_photo_count` int(11) DEFAULT NULL,
  `like_count` int(11) DEFAULT NULL,
  `asso_like_count` int(11) DEFAULT NULL,
  `follower_count` int(11) DEFAULT NULL,
  `play_count` int(11) DEFAULT NULL,
  `is_follow_object` tinyint(1) DEFAULT NULL,
  `isActionType` tinyint(1) DEFAULT NULL,
  `object_id` varchar(128) DEFAULT NULL,
  `object` text CHARACTER SET utf8mb4,
  `card_info_un_integrity` tinyint(1) DEFAULT NULL,
  `info` text,
  `expire_time` int(11) DEFAULT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `pid_url_ori_UNIQUE` (`pid`,`url_ori`),
  KEY `weibo_media_post_idx` (`pid`),
  CONSTRAINT `weibo_media_post` FOREIGN KEY (`pid`) REFERENCES `weibo_post_status` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `weibo_post_status`
--

DROP TABLE IF EXISTS `weibo_post_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo_post_status` (
  `id` bigint(20) NOT NULL,
  `uid` bigint(20) NOT NULL,
  `text` text CHARACTER SET utf8mb4,
  `favorited` tinyint(1) DEFAULT NULL,
  `truncated` tinyint(1) DEFAULT NULL,
  `reposts_count` bigint(20) DEFAULT NULL,
  `comments_count` bigint(20) DEFAULT NULL,
  `attitudes_count` bigint(20) DEFAULT NULL,
  `in_reply_to_status_id` varchar(128) DEFAULT NULL,
  `in_reply_to_user_id` varchar(128) DEFAULT NULL,
  `in_reply_to_screen_name` varchar(128) DEFAULT NULL,
  `geo` varchar(64) DEFAULT NULL,
  `mid` varchar(64) NOT NULL,
  `annotations` text,
  `appid` int(11) DEFAULT NULL,
  `biz_feature` bigint(20) DEFAULT NULL,
  `biz_ids` varchar(128) DEFAULT NULL,
  `bmiddle_pic` varchar(128) DEFAULT NULL,
  `can_edit` tinyint(1) DEFAULT NULL,
  `created_at` varchar(128) NOT NULL,
  `comment_manage_info` varchar(128) DEFAULT NULL,
  `content_auth` int(11) DEFAULT NULL,
  `darwin_tags` varchar(128) DEFAULT NULL,
  `expire_time` varchar(128) DEFAULT NULL,
  `extend_info` text,
  `gif_ids` text,
  `hasActionTypeCard` tinyint(1) DEFAULT NULL,
  `hot_weibo_tags` varchar(128) DEFAULT NULL,
  `idstr` varchar(128) DEFAULT NULL,
  `isLongText` tinyint(1) DEFAULT NULL,
  `is_paid` tinyint(1) DEFAULT NULL,
  `is_show_bulletin` int(11) DEFAULT NULL,
  `mblog_vip_type` int(11) DEFAULT NULL,
  `mlevel` int(11) DEFAULT NULL,
  `more_info_type` int(11) DEFAULT NULL,
  `original_pic` varchar(128) DEFAULT NULL,
  `page_type` varchar(128) DEFAULT NULL,
  `pending_approval_count` int(11) DEFAULT NULL,
  `picStatus` varchar(128) DEFAULT NULL,
  `pic_ids` text,
  `pid` varchar(128) DEFAULT NULL,
  `positive_recom_flag` varchar(128) DEFAULT NULL,
  `is_retweeted` tinyint(1) DEFAULT NULL,
  `retweeted_id` bigint(20) DEFAULT NULL,
  `rid` varchar(128) DEFAULT NULL,
  `source` varchar(128) DEFAULT NULL,
  `has_url_objects` tinyint(1) DEFAULT NULL,
  `source_allowclick` int(11) DEFAULT NULL,
  `source_type` int(11) DEFAULT NULL,
  `textLength` varchar(128) DEFAULT NULL,
  `text_tag_tips` varchar(128) DEFAULT NULL,
  `thumbnail_pic` varchar(128) DEFAULT NULL,
  `userType` int(11) DEFAULT NULL,
  `visible` varchar(128) DEFAULT NULL,
  `filterID` varchar(128) DEFAULT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `mid` (`mid`),
  KEY `INX_WEIBO_POST_USER_ID` (`uid`),
  CONSTRAINT `weibo_post_weibo_user_info_uid` FOREIGN KEY (`uid`) REFERENCES `weibo_user_info` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `weibo_user_growth_daily`
--

DROP TABLE IF EXISTS `weibo_user_growth_daily`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo_user_growth_daily` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `uid` bigint(20) NOT NULL,
  `followers_count` bigint(20) DEFAULT NULL,
  `friends_count` bigint(20) DEFAULT NULL,
  `statuses_count` bigint(20) DEFAULT NULL,
  `private_friends_count` bigint(20) DEFAULT NULL,
  `pagefriends_count` bigint(20) DEFAULT NULL,
  `update_date` date NOT NULL,
  `year` int(11) NOT NULL,
  `month` int(11) NOT NULL,
  `day` int(11) NOT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uid_UNIQUE` (`uid`,`update_date`),
  CONSTRAINT `weibo_user_growth_user_info` FOREIGN KEY (`uid`) REFERENCES `weibo_user_info` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=31 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `weibo_user_growth_monthly`
--

DROP TABLE IF EXISTS `weibo_user_growth_monthly`;
/*!50001 DROP VIEW IF EXISTS `weibo_user_growth_monthly`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE VIEW `weibo_user_growth_monthly` AS SELECT 
 1 AS `year`,
 1 AS `month`,
 1 AS `uid`,
 1 AS `followers_count`,
 1 AS `friends_count`,
 1 AS `statuses_count`,
 1 AS `private_friends_count`,
 1 AS `pagefriends_count`*/;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `weibo_user_info`
--

DROP TABLE IF EXISTS `weibo_user_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo_user_info` (
  `uid` bigint(20) NOT NULL,
  `allow_all_act_msg` tinyint(1) DEFAULT NULL,
  `allow_all_comment` tinyint(1) DEFAULT NULL,
  `avatar_hd` longtext,
  `avatar_large` longtext,
  `bi_followers_count` bigint(20) DEFAULT NULL,
  `block_app` int(11) DEFAULT NULL,
  `block_word` int(11) DEFAULT NULL,
  `city` int(11) DEFAULT NULL,
  `class` int(11) DEFAULT NULL,
  `cover_image` longtext,
  `cover_image_phone` longtext,
  `created_at` varchar(32) DEFAULT NULL,
  `credit_score` int(11) DEFAULT NULL,
  `description` longtext CHARACTER SET utf8mb4,
  `domain` varchar(128) DEFAULT NULL,
  `favourites_count` bigint(20) DEFAULT NULL,
  `following` tinyint(1) DEFAULT NULL,
  `follow_me` tinyint(1) DEFAULT NULL,
  `followers_count` bigint(20) DEFAULT NULL,
  `friends_count` bigint(20) DEFAULT NULL,
  `gender` varchar(128) DEFAULT NULL,
  `geo_enabled` tinyint(1) DEFAULT NULL,
  `has_service_tel` varchar(128) DEFAULT NULL,
  `idstr` varchar(128) DEFAULT NULL,
  `insecurity` varchar(128) DEFAULT NULL,
  `lang` varchar(128) DEFAULT NULL,
  `like` tinyint(1) DEFAULT NULL,
  `like_me` tinyint(1) DEFAULT NULL,
  `location` varchar(128) DEFAULT NULL,
  `mbrank` int(11) DEFAULT NULL,
  `mbtype` int(11) DEFAULT NULL,
  `name` varchar(32) DEFAULT NULL,
  `online_status` tinyint(1) DEFAULT NULL,
  `pagefriends_count` bigint(20) DEFAULT NULL,
  `profile_image_url` longtext,
  `profile_url` text,
  `province` int(11) DEFAULT NULL,
  `ptype` int(11) DEFAULT NULL,
  `remark` varchar(128) DEFAULT NULL,
  `screen_name` varchar(128) DEFAULT NULL,
  `star` int(11) DEFAULT NULL,
  `statuses_count` bigint(20) DEFAULT NULL,
  `story_read_state` int(11) DEFAULT NULL,
  `urank` int(11) DEFAULT NULL,
  `url` varchar(128) DEFAULT NULL,
  `user_ability` bigint(20) DEFAULT NULL,
  `vclub_member` tinyint(1) DEFAULT NULL,
  `verified` tinyint(1) DEFAULT NULL,
  `verified_contact_email` varchar(128) DEFAULT NULL,
  `verified_contact_mobile` varchar(128) DEFAULT NULL,
  `verified_contact_name` varchar(128) DEFAULT NULL,
  `verified_level` varchar(128) DEFAULT NULL,
  `verified_reason` varchar(128) DEFAULT NULL,
  `verified_reason_modified` varchar(128) DEFAULT NULL,
  `verified_reason_url` varchar(128) DEFAULT NULL,
  `verified_source` varchar(128) DEFAULT NULL,
  `verified_source_url` varchar(128) DEFAULT NULL,
  `verified_state` varchar(128) DEFAULT NULL,
  `verified_trade` varchar(128) DEFAULT NULL,
  `verified_type` int(11) DEFAULT NULL,
  `verified_type_ext` varchar(128) DEFAULT NULL,
  `weihao` varchar(128) DEFAULT NULL,
  `enterprise_name` varchar(128) DEFAULT NULL,
  `pay_date` varchar(128) DEFAULT NULL,
  `pay_remind` varchar(128) DEFAULT NULL,
  `ulevel` varchar(128) DEFAULT NULL,
  `level` varchar(128) DEFAULT NULL,
  `has_ability_tag` varchar(128) DEFAULT NULL,
  `extend` varchar(128) DEFAULT NULL,
  `badge_top` varchar(128) DEFAULT NULL,
  `badge` text,
  `cardid` varchar(128) DEFAULT NULL,
  `avatargj_id` varchar(128) DEFAULT NULL,
  `type` varchar(128) DEFAULT NULL,
  `attitude_style` varchar(128) DEFAULT NULL,
  `ability_tags` varchar(128) DEFAULT NULL,
  `unicom_free_pc` varchar(128) DEFAULT NULL,
  `dianping` text CHARACTER SET utf8mb4,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `weibo_user_tag`
--

DROP TABLE IF EXISTS `weibo_user_tag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `weibo_user_tag` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `uid` bigint(20) NOT NULL,
  `tag_id` varchar(128) NOT NULL,
  `tag_name` varchar(128) CHARACTER SET utf8mb4 DEFAULT NULL,
  `weight` int(11) DEFAULT NULL,
  `flag` tinyint(1) DEFAULT NULL,
  `created_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `weibo_tag_user_info_idx` (`uid`),
  CONSTRAINT `weibo_tag_user_info` FOREIGN KEY (`uid`) REFERENCES `weibo_user_info` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Final view structure for view `weibo_user_growth_monthly`
--

/*!50001 DROP VIEW IF EXISTS `weibo_user_growth_monthly`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `weibo_user_growth_monthly` AS select `a`.`year` AS `year`,`b`.`month` AS `month`,`a`.`uid` AS `uid`,`a`.`followers_count` AS `followers_count`,`a`.`friends_count` AS `friends_count`,`a`.`statuses_count` AS `statuses_count`,`a`.`private_friends_count` AS `private_friends_count`,`a`.`pagefriends_count` AS `pagefriends_count` from (`pandas`.`weibo_user_growth_daily` `a` join (select `pandas`.`weibo_user_growth_daily`.`uid` AS `uid`,`pandas`.`weibo_user_growth_daily`.`year` AS `year`,`pandas`.`weibo_user_growth_daily`.`month` AS `month`,max(`pandas`.`weibo_user_growth_daily`.`day`) AS `last_day` from `pandas`.`weibo_user_growth_daily` group by `pandas`.`weibo_user_growth_daily`.`uid`,`pandas`.`weibo_user_growth_daily`.`year`,`pandas`.`weibo_user_growth_daily`.`month`) `b`) where ((`a`.`uid` = `b`.`uid`) and (`a`.`year` = `b`.`year`) and (`a`.`month` = `b`.`month`) and (`a`.`day` = `b`.`last_day`)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-01-27 15:50:06
