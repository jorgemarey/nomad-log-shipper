@Library(["workflowlibs"]) _
@Library('meigas')

// Load globalHelper
globalHelper = new globaldevops.helper.helper()
go = new meigas.meigasGo.meigasGo();

PROJECT="nomad-log-shipper"
VERSION="0.1.3"

// Initilize status
currentBuild.result = 'SUCCESS'

node ('meigas-golang'){    
    try{
        gobuildOutput= go.build("${PROJECT}", "${VERSION}")
        echo "${gobuildOutput}"
    } catch (Exception e) {
        currentBuild.result = 'FAILED'
        echo "${env.JOB_NAME} - Build # ${env.BUILD_NUMBER} - FAILURE (${e.message})!"
        return
    }
}