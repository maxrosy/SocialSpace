# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
from datetime import datetime


class Social(models.Model):
	tagid = models.BigAutoField(db_column='tagID', primary_key=True)  # Field name made lowercase.
	agency = models.CharField(db_column='Agency', max_length=50, blank=True, null=True)  # Field name made lowercase.
	platform = models.CharField(db_column='Platform', max_length=50, blank=True, null=True)  # Field name made lowercase.
	url = models.TextField(db_column='Url', blank=True, null=True)  # Field name made lowercase.
	date_sampled = models.DateTimeField(db_column='Date Sampled', blank=True, null=True)  # Field name made lowercase. Field renamed to remove unsuitable characters.
	likes = models.IntegerField(db_column='Likes', blank=True, null=True)  # Field name made lowercase.

	class Meta:
		managed = False
		db_table = 'social'


class TaskHistory(models.Model):
	task_id = models.BigIntegerField(primary_key=True)
	user_id = models.CharField(max_length=64)
	secret_key = models.CharField(max_length=64)
	status = models.BooleanField()
	created_time = models.DateTimeField()
	updated_time = models.DateTimeField()

	class Meta:
		managed = False
		db_table = 'task_history'
	
	def save(self, *args, **kwargs):
		self.updated_time = datetime.now()
		super().save(*args, **kwargs)
		
	def __str__(self):
		return str(self.task_id)

"""
class Wechat(models.Model):
    cancel_user = models.BigIntegerField(blank=True, null=True)
    new_user = models.BigIntegerField(blank=True, null=True)
    ref_date = models.TextField(blank=True, null=True)
    user_source = models.BigIntegerField(blank=True, null=True)
    created_time = models.DateTimeField()
    updated_time = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'wechat'


class Weibo(models.Model):
    allow_all_act_msg = models.IntegerField(blank=True, null=True)
    allow_all_comment = models.IntegerField(blank=True, null=True)
    avatar_large = models.TextField(blank=True, null=True)
    bi_followers_count = models.BigIntegerField(blank=True, null=True)
    block_word = models.BigIntegerField(blank=True, null=True)
    city = models.BigIntegerField(blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    domain = models.TextField(blank=True, null=True)
    favourites_count = models.BigIntegerField(blank=True, null=True)
    follow_me = models.IntegerField(blank=True, null=True)
    followers_count = models.BigIntegerField(blank=True, null=True)
    following = models.IntegerField(blank=True, null=True)
    friends_count = models.BigIntegerField(blank=True, null=True)
    gender = models.TextField(blank=True, null=True)
    geo_enabled = models.IntegerField(blank=True, null=True)
    id = models.BigIntegerField(blank=True, null=True)
    idstr = models.BigIntegerField(blank=True, null=True)
    lang = models.TextField(blank=True, null=True)
    location = models.TextField(blank=True, null=True)
    mbrank = models.BigIntegerField(blank=True, null=True)
    mbtype = models.BigIntegerField(blank=True, null=True)
    name = models.TextField(blank=True, null=True)
    online_status = models.BigIntegerField(blank=True, null=True)
    profile_image_url = models.TextField(blank=True, null=True)
    profile_url = models.BigIntegerField(blank=True, null=True)
    province = models.BigIntegerField(blank=True, null=True)
    remark = models.TextField(blank=True, null=True)
    screen_name = models.TextField(blank=True, null=True)
    star = models.BigIntegerField(blank=True, null=True)
    status_id = models.BigIntegerField(blank=True, null=True)
    statuses_count = models.BigIntegerField(blank=True, null=True)
    url = models.TextField(blank=True, null=True)
    verified = models.IntegerField(blank=True, null=True)
    verified_reason = models.TextField(blank=True, null=True)
    verified_type = models.BigIntegerField(blank=True, null=True)
    weihao = models.BigIntegerField(blank=True, null=True)
    created_time = models.DateTimeField()
    updated_time = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'weibo'
"""