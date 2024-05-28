TODO
####

Syncing Fork and rebasing Pull request
--------------------------------------

Often it takes several days or weeks to discuss and iterate with the PR until it is ready to merge.
In the meantime new commits are merged, and you might run into conflicts, therefore you should periodically
synchronize main in your fork with the ``apache/airflow`` main and rebase your PR on top of it. Following
describes how to do it.

* `Update new changes made to apache:airflow project to your fork <10_working_with_git.rst#how-to-sync-your-fork>`__
* `Rebasing pull request <10_working_with_git.rst#how-to-rebase-pr>`__


Raising Pull Request
--------------------

1. Go to your GitHub account and open your fork project and click on Branches

   .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/quick_start/pr1.png"
           alt="Goto fork and select branches">
    </div>

2. Click on ``New pull request`` button on branch from which you want to raise a pull request.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr2.png"
             alt="Accessing local airflow">
      </div>

3. Add title and description as per Contributing guidelines and click on ``Create pull request``.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr3.png"
             alt="Accessing local airflow">
      </div>