package controllers;

import javax.annotation.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.faces.event.ActionEvent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import services.TaskService;


@ManagedBean
@Component
@RequestScoped
public class TaskController {
    @Autowired
    private TaskService taskService;

    public void startTasks(ActionEvent event) throws InterruptedException {
        this.taskService.startIdleTasks();
    }

}