@Library(["workflowlibs"]) _
// Load globalHelper
globalHelper = new globaldevops.helper.helper()

// Initilize status
currentBuild.result = 'SUCCESS'

@Library('meigas')
import meigas.meigasGo.meigasGo;
def meigasgo = new meigasGo();

VERSION="0.0.1"
REPO_NAME="nomad-log-shipper"
ARCHITECTURE="amd64"

node ('meigas-golang'){    
    try{
        gobuildOutput= meigasgo.goBuild("${REPO_NAME}", "${VERSION}", "${ARCHITECTURE}", "${env.jenkins_ssh_credentials}", "${env.BRANCH_NAME}")
        echo "${gobuildOutput}"
    } catch (Exception e) {
        echo "${env.JOB_NAME} - Build # ${env.BUILD_NUMBER} - FAILURE (${e.message})!"
        currentBuild.result = 'FAILED'
        return
    }
}