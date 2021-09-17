@Library('cicd') _
 pipeline {
    agent any
    stages {
        stage('Build and Publish Gem'){
            steps{
                buildGem(false)
            }
        }
   }
}