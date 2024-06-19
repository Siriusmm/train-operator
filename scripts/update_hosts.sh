#!/bin/sh
timeout=3600
cat /etc/deepspeed/hostfile | while IFS= read -r line; do
    while true; do
        pod_name=$(echo "$line" | awk '{print $1}')
        pod_ip=$(/opt/kube/kubectl get pod "$pod_name" -n default -o jsonpath='{.status.podIP}')
        pod_status=$(/opt/kube/kubectl get pod "$pod_name" -n default -o jsonpath='{.status.phase}')
        if [ -n "$pod_ip" ] && [ "$pod_status" == "Running" ]; then
            echo $pod_ip
            break
        else
            echo 'wait for pod to start'
            timeout=$(($timeout-1))
            sleep 1
        fi
        echo $timeout
        if [ $timeout -eq 0 ];then
           echo 'timeout for get pod ip'
           break
        fi
    done
    echo "$pod_ip $pod_name" >> /etc/hosts
done