@Library('cicd@clusterName') _
 pipeline {
    agent any
    stages {
        stage('Build and Publish Gem'){
            steps{
                buildGem( "fluent-plugin-zebrium_output")
            }
        }
   }
}