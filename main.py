import luigi
from luigi.contrib.s3 import S3Target

BUCKET = 'alex-luigi-test-bucket' ## Update this to your bucket name


# Each run of the script should call out to a class that subclasses luigi.Task
class MyS3Report(luigi.Task):
    
    # This Task subclass will take some parameters that are specific to each run, such as a region, table name, or account ID.
    parameter1 = luigi.Parameter()
    parameter2 = luigi.IntParameter()

    # output() is how you will know this task is complete. It should include the parameter names for each run.
    # In this example, there will be a file at s3://alex-luigi-test-bucket/example-1.json when my task runs with paramer1 = 'example' and parameter2 = '1'.
    # If this file already exists, the Task knows it is complete and won't run.
    def output(self):
        return S3Target('s3://{bucket}/{parameter1}-{parameter2}.json'.format(
            bucket=BUCKET,
            parameter1=self.parameter1,
            parameter2=self.parameter2
        ))

    # run() is the method that will execute a single task based on its parameters.
    # You will have your script run here.
    # Then, you will write the results of your script to the output() file using the syntax below to open and write to it.
    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("Final output for {parameter1} & {parameter2}".format(
                parameter1=self.parameter1,
                parameter2=self.parameter2
            ))



# A WrapperTask doesn't have it any outputs itself, but it checks for the completion of all its subtasks.
# You would yield an instance of your subclassed Task for each run of the script you wanted to do.
# The AllReports task will be finished when all its subtasks are complete.
class AllReports(luigi.WrapperTask):
    def requires(self):
        for i in range(3):
            yield MyS3Report(parameter1='example', parameter2=i)


# This runs the task. Execute with "python main.py".
# If you run this in Lambda, you would put luigi.run() in your handler function.
if __name__ == "__main__":
    luigi.run(main_task_cls=AllReports, local_scheduler=True)
