# SkycastAnalyticsDeletionTask
Task for Deleting Analytics Blobs that have expired

### Current Installation
The application is currently installed on the Jenkins server on the Ratio corporate network. It is executed on a nightly schedule via a Scheduled Task. The task executes a Windows batch script, which in turn runs the application and logs all output to a log file. A new log file is generated each time the task runs, and includes the date and time of execution in the file name. All log files, and the batch script are located in the C:\Skycast directory of the Jenkins machine.
