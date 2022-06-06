
nextflow.preview.dsl = 2

usage = file("${workflow.scriptFile.getParent()}/usage/bids_usage")
bindings = [ "rewrite":"$params.rewrite",
             "subjects":"$params.subjects",
             "simg":"$params.simg",
             "descriptor":"$params.descriptor",
             "invocation":"$params.invocation",
             "license":"$params.license",
             "resources": "$params.resources"]

engine = new groovy.text.SimpleTemplateEngine()
toprint = engine.createTemplate(usage.text).make(bindings)
printhelp = params.help

// Input checking and validation
req_param = ["--bids" : params.bids,
             "--simg" : params.simg,
             "--license": params.license,
             "--descriptor": params.descriptor,
             "--invocation": params.invocation,
             "--out": params.out]

missing_arg = req_param.grep{ (it.value == null || it.value == "") }
if (missing_arg){
    log.error("Missing required argument(s)!")
    missing_arg.each{ log.error("Missing ${it.key}") }
    printhelp = true
}

if (printhelp){
   print(toprint)
   System.exit(0)
}


process save_invocation{

    input:
    path invocation

    shell:
    '''

    invoke_name=$(basename !{params.invocation})
    invoke_name=${invoke_name%.json}
    datestr=$(date +"%d-%m-%Y")

    # If file with same date is available, check if they are the same
    if [ -f !{params.out}/${invoke_name}_${datestr}.json ]; then

        DIFF=$(diff !{params.invocation} !{params.out}/${invoke_name}_${datestr}.json)

        if [ "$DIFF" != "" ]; then
            >&2 echo "Error invocations have identical names but are not identical!"
            exit 1
        fi

    else
        cp -n !{params.invocation} !{params.out}/${invoke_name}_${datestr}.json
    fi
    '''
}


process modify_invocation{

    input:

    val sub
    output:
    tuple val(sub), path("${sub}.json"), emit: json

    """

    #!/usr/bin/env python

    import json
    import sys

    out_file = '${sub}.json'
    invoke_file = '${params.invocation}'

    x = '${sub}'.replace('sub-','')

    with open(invoke_file,'r') as f:
        j_dict = json.load(f)

    j_dict.update({'participant_label' : [x]})

    with open(out_file,'w') as f:
        json.dump(j_dict,f,indent=4)

    """
}


process run_bids{

    time { params.cluster_time(s) }
    queue { params.cluster_queue(s) }

    input:
    tuple path(sub_input), val(s)

    scratch params.scratchDir
    stageInMode 'copy'

    shell:
    '''

    #Stop error rejection
    set +e

    #Make logging folder
    logging_dir=!{params.out}/pipeline_logs/!{params.application}
    mkdir -p ${logging_dir}

    #Set up logging output
    sub_json=!{sub_input}
    sub=${sub_json%.json}
    datestr=!{
        workflow.start.toString().split("T")[0]
    }
    log_out=${logging_dir}/${sub}_${datestr}.out
    log_err=${logging_dir}/${sub}_${datestr}.err


    echo "TASK ATTEMPT !{task.attempt}" >> ${log_out}
    echo "============================" >> ${log_out}
    echo "TASK ATTEMPT !{task.attempt}" >> ${log_err}
    echo "============================" >> ${log_err}

    mkdir work
    bosh exec launch \
    -v !{params.bids}:/bids \
    -v !{params.out}:/output \
    -v !{params.license}:/license \
    !{ (params.resources) ? "-v $params.resources:/resources" : ""} \
    -v $(pwd)/work:/work \
    !{params.descriptor} $(pwd)/!{sub_input} \
    --imagepath !{params.simg} -x --stream 2>> ${log_out} \
                                           1>> ${log_err}

    '''
}

// Helpful logging information
log.info("BIDS Directory: $params.bids")
log.info("Output directory: $params.out")
log.info("Using Descriptor File: $params.descriptor")
log.info("Using Invocation File: $params.invocation")
log.info("Using scratch directory: $params.scratchDir")

input_channel = Channel.fromPath("$params.bids/sub-*", type: 'dir')
                       .map { i -> i.getBaseName() }

// If using --subjects, apply filter
if (params.subjects){
    subjects_channel = Channel.fromPath(params.subjects)
                               .splitText() { it.strip() }
    input_channel = input_channel.join(subjects_channel)
}

if (!params.rewrite){

    // The "o" trick is to ensure that nulls get placed
    // in it[1] when joining
    out_channel = Channel.fromPath("$params.out/$params.application/sub-*", type: 'dir')
                        .map{ o -> [o.getBaseName(), "o"] }
                        .ifEmpty(['', "o"])

    input_channel = input_channel.join(out_channel, remainder: true)
                                 .filter{it.last() == null}
                                 .map{ i,n -> i }
}

workflow {

    main:

    // Pull # of sessions per subject
    sub_ses_channel = input_channel
                        .map{ s ->[
                                   s,
                                   new File("$params.bids/$s/").listFiles().size()
                                  ]
                            }

    save_invocation(params.invocation)
    modify_invocation(input_channel)

    run_bids_input = modify_invocation.out.json
                                .join(sub_ses_channel, by: 0)
                                .map { s,i,n -> [i, n]}
    run_bids(run_bids_input)
}
