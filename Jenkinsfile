// Jenkinsfile
// This is the CI pipeline for the crypto-pipeline project

pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'YOURUSERNAME/crypto-pipeline'
        DOCKER_TAG   = "v${BUILD_NUMBER}"
        SONAR_PROJECT = 'crypto-pipeline'
    }

    stages {

        stage('Checkout') {
            steps {
                // Pull the latest code from GitHub
                checkout scm
                echo 'Code checked out successfully'
            }
        }

        stage('SonarQube Analysis') {
            steps {
                withSonarQubeEnv('sonarqube') {
                    sh '''
                        sonar-scanner \
                          -Dsonar.projectKey=${crypto-pipeline} \
                          -Dsonar.sources=src \
                          -Dsonar.language=py \
                          -Dsonar.host.url=http://localhost:9000 \
                          -Dsonar.login=sqa_f32b5eb4bf420137f3d42a3ff3ac915368d6fd3d
                    '''
                }
            }
        }

        stage('Quality Gate') {
            steps {
                // Wait for SonarQube to finish analysis
                // If quality gate fails, this stage fails and deploy is blocked
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                echo "Building image: ${DOCKER_IMAGE}:${DOCKER_TAG}"
                sh 'docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} .'
                sh 'docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${DOCKER_IMAGE}:latest'
            }
        }

        stage('Push to DockerHub') {
            steps {
                withCredentials([usernamePassword(
                    credentialsId: 'dockerhub-creds',
                    usernameVariable: 'DOCKER_USER',
                    passwordVariable: 'DOCKER_PASS'
                )]) {
                    sh 'echo $DOCKER_PASS | docker login -u $DOCKER_USER --password-stdin'
                    sh 'docker push ${DOCKER_IMAGE}:${DOCKER_TAG}'
                    sh 'docker push ${DOCKER_IMAGE}:latest'
                }
            }
        }

    }

    post {
        success {
            echo 'CI Pipeline passed! Docker image pushed to DockerHub.'
        }
        failure {
            echo 'CI Pipeline FAILED. Check the stage that failed above.'
        }
    }
}
