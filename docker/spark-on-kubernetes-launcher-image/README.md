To build the docker image you need the following variables set:
- PACKAGE_REPO_URL
- PACKAGE_REPO_USER
- GITLAB_USER
- GITLAB_PRIVATE_TOKEN

you also need a txt file containing just your PACKAGE_REPO_PASS in plain text with no parentheses in

To build the image run:

```
docker build . -t=pachyderm-spark --build-arg=PACKAGE_REPO_URL=$PACKAGE_REPO_URL --build-arg=PACKAGE_REPO_USER=$PACKAGE_REPO_USER --build-arg=GITLAB_USER=$GITLAB_USER --build-arg=GITLAB_PRIVATE_TOKEN=$GITLAB_PRIVATE_TOKEN --secret id=artifactory_password,src=/FULL/PATH/TO/password.txt
```

IMPORTANT:
For development please remember to change the:
ARG --build-arg pyspark_pipeline_BRANCH=master
value in the docker file to the branch you are testing on. In order to test your changes.
