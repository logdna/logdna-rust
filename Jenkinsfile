library 'magic-butler-catalogue'
def PROJECT_NAME = 'logdna-rust'
def TRIGGER_PATTERN = '.*@logdnabot.*'
def publishImage = false

pipeline {
    agent any
    options {
        timestamps()
        ansiColor 'xterm'
    }
    triggers {
        issueCommentTrigger(TRIGGER_PATTERN)
        cron(env.BRANCH_NAME ==~ /\d\.\d/ ? 'H H 1,15 * *' : '')
    }
    environment {
        RUST_IMAGE_REPO = 'us.gcr.io/logdna-k8s/rust'
        RUST_IMAGE_TAG = 'buster-1-stable'
        SCCACHE_BUCKET = 'logdna-sccache-us-west-2'
        SCCACHE_REGION = 'us-west-2'
        CARGO_INCREMENTAL = 'false'
    }
    stages {
        stage('Validate PR Source') {
          when {
            expression { env.CHANGE_FORK }
            not {
                triggeredBy 'issueCommentCause'
            }
          }
          steps {
            error("A maintainer needs to approve this PR for CI by commenting")
          }
        }
        stage('Pull Build Image') {
            steps {
                sh "docker pull ${RUST_IMAGE_REPO}:${RUST_IMAGE_TAG}"
            }
        }
        stage ("Lint and Test"){
            environment {
                CREDS_FILE = credentials('pipeline-e2e-creds')
                LOGDNA_HOST = "logs.use.stage.logdna.net"
            }
            steps {
                script {
                    def creds = readJSON file: CREDS_FILE
                    // Assumes the pipeline-e2e-creds format remains the same. Chase
                    // refer to the e2e tests's README's authorization docs for the
                    // current structure
                    LOGDNA_INGESTION_KEY = creds["packet-stage"]["account"]["ingestionkey"]
                }
                withCredentials([[
                    $class: 'AmazonWebServicesCredentialsBinding',
                    credentialsId: 'aws',
                    accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                    secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                ]]){
                    sh """
                        make lint
                        make LOGDNA_INGESTION_KEY=${LOGDNA_INGESTION_KEY} test
                    """
                }
            }
            post {
                success {
                    sh "make clean"
                }
            }
        }
    }
}
