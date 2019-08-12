@Library(["workflowlibs"]) _
// Load globalHelper
globalHelper = new globaldevops.helper.helper()

// Initilize status
currentBuild.result = 'SUCCESS'

@Library('meigas')
import meigas.meigasGo.meigasGo;
def meigasgo = new meigasGo();

VERSION="0.1.1"
REPO_NAME="nomad-log-shipper"

node ('meigas-golang'){    
    try{
        gobuildOutput= meigasgo.build("${REPO_NAME}", "${VERSION}")
        echo "${gobuildOutput}"
    } catch (Exception e) {
        echo "${env.JOB_NAME} - Build # ${env.BUILD_NUMBER} - FAILURE (${e.message})!"
        currentBuild.result = 'FAILED'
        return
    }
}