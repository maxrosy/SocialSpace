from django.shortcuts import render,get_object_or_404

# Create your views here.
from .models import TaskHistory
from django.views.decorators.cache import cache_page

@cache_page(60*15)
def index(request):
	task_list = TaskHistory.objects.order_by('-created_time')
	context = {
		'task_list':task_list,
	}
	return render(request,'tasks/index.html',context)

@cache_page(60*15)
def detail(request, task_id):
	task = get_object_or_404(TaskHistory,pk=task_id)
	context = {
		'task':task
	}
	return render(request, 'tasks/detail.html',context)
	
	