from django.contrib import admin

# Register your models here.
from datetime import datetime
from . import models

class TaskHistoryAdmin(admin.ModelAdmin):
	fields = [('task_id','secret_key','user_id'),'status','created_time','updated_time']
	readonly_fields = ['task_id','secret_key','user_id','created_time','updated_time']
	
	def save_model(self,request,obj,form,change):
		obj.updated_time = datetime.now()
		super().save_model(request,obj,form,change)
	

admin.site.register(models.TaskHistory,TaskHistoryAdmin)