#!/bin/bash

# VPA Consolidated Resource Request Setter
# 
# This script extracts VPA (Vertical Pod Autoscaler) recommendations and applies them 
# as resource requests directly to individual pods in the corresponding workloads 
# (Deployments, StatefulSets, DaemonSets). Supports Target, UpperBound, and LowerBound recommendations.
#
# The script works by:
# 1. Finding all VPA objects in specified namespace(s)
# 2. Extracting the specified recommendation type (target/upperbound/lowerbound) for each container
# 3. Identifying the target workload (Deployment/StatefulSet/DaemonSet)
# 4. Finding all pods belonging to that workload
# 5. Directly patching each pod's resource requests using kubectl patch --subresource resize
#
# IMPORTANT NOTES:
# - This sets REQUESTS (not limits) based on VPA recommendations
# - Uses kubectl patch --subresource resize for in-place pod resource updates
# - Target: recommended optimal resources for efficient operation
# - UpperBound: maximum recommended resources for safe operation (HPA compatible)
# - LowerBound: minimum recommended resources to avoid resource starvation
# - Only containers with VPA recommendations will be updated
# - Existing resource limits are preserved and unchanged
# - Patches individual pods directly, not the workload specs
#
# Usage:
#   ./set-pod-requests-from-vpa.sh [--target|--upperbound|--lowerbound] [namespace] [--dry-run] [--pod-healthy-duration MINUTES] [--not-older-than MINUTES]
#   ./set-pod-requests-from-vpa.sh [--target|--upperbound|--lowerbound] --all-namespaces [--dry-run] [--pod-healthy-duration MINUTES] [--exclude-my-app] [--not-older-than MINUTES]
#
# Options:
#   --target           Use VPA Target recommendations (default)
#   --upperbound       Use VPA UpperBound recommendations (HPA compatible)
#   --lowerbound       Use VPA LowerBound recommendations
#   namespace          Target specific namespace (default: my-app)
#   --all-namespaces   Process all user namespaces (excludes system namespaces)
#   --dry-run         Show what changes would be made without applying them
#   --pod-healthy-duration MINUTES Only process pods that have been healthy for at least MINUTES (default: 5)
#   --not-older-than MINUTES Only process pods created in the past MINUTES (overrides --pod-healthy-duration)
#   --exclude-my-app   When used with --all-namespaces, also excludes 'my-app' namespace
#
# Examples:
#   ./set-pod-requests-from-vpa.sh --target                           # Process 'my-app' namespace with target recommendations
#   ./set-pod-requests-from-vpa.sh --upperbound my-app                # Process 'my-app' namespace with upperbound recommendations
#   ./set-pod-requests-from-vpa.sh --lowerbound --all-namespaces      # Process all user namespaces with lowerbound recommendations
#   ./set-pod-requests-from-vpa.sh --upperbound --dry-run             # Dry run for 'my-app' namespace with upperbound
#   ./set-pod-requests-from-vpa.sh --target --pod-healthy-duration 10             # Only pods healthy for 10+ minutes with target
#   ./set-pod-requests-from-vpa.sh --upperbound --not-older-than 30              # Only pods created in past 30 minutes with upperbound
#   ./set-pod-requests-from-vpa.sh --upperbound --all-namespaces --exclude-my-app --dry-run # Upperbound, all except my-app, dry run
#
# Prerequisites:
# - kubectl access to the cluster
# - VPA objects must exist and have recommendations
# - Sufficient RBAC permissions to patch workloads
# - bc command for memory unit conversions

NAMESPACE=""
ALL_NAMESPACES=false
DRY_RUN=false
DURATION_MINUTES="5"  # Default to 5 minutes for pod stability
EXCLUDE_MY_APP=false
NOT_OLDER_THAN_MINUTES=""
RECOMMENDATION_TYPE="upperbound"  # Default to upperbound

# Generate timestamp for logging
TIMESTAMP=$(date +"%m-%d-%H-%M")

# Get cluster name for file prefixing
CLUSTER_NAME=$(kubectl config current-context 2>/dev/null | sed 's/.*\///g' | sed 's/[^a-zA-Z0-9-]/_/g' || echo "unknown-cluster")

# Generate log and CSV file names with cluster prefix and recommendation type
LOG_FILE="${CLUSTER_NAME}_vpa-${RECOMMENDATION_TYPE}-requests-${TIMESTAMP}.log"
CSV_FILE="${CLUSTER_NAME}_set-requests-from-vpa-${RECOMMENDATION_TYPE}-${TIMESTAMP}.csv"

# Function to log messages with timestamp
log_message() {
    local message="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $message" | tee -a "$LOG_FILE"
}

# Initialize CSV file with header
init_csv() {
    if [[ "$RECOMMENDATION_TYPE" == "upperbound" ]]; then
        echo "Timestamp,Namespace,VPA_Name,Target_Kind,Target_Name,Pod_Name,Container_Name,Current_CPU_Request,Current_Memory_Request,Actual_CPU_Usage,Actual_Memory_Usage,UpperBound_CPU,UpperBound_Memory,UpperBound_Memory_K8s,Status,Error_Message" > "$CSV_FILE"
    else
        echo "Timestamp,Namespace,VPA_Name,Target_Kind,Target_Name,Pod_Name,Container_Name,Current_CPU_Request,Current_Memory_Request,${RECOMMENDATION_TYPE^}_CPU,${RECOMMENDATION_TYPE^}_Memory,${RECOMMENDATION_TYPE^}_Memory_K8s,Status,Error_Message" > "$CSV_FILE"
    fi
    echo "CSV output will be written to: $CSV_FILE"
}

# Function to get actual CPU/memory usage for upperbound mode
get_actual_usage() {
    local namespace="$1"
    local pod="$2"
    local container_name="$3"
    
    if [[ "$RECOMMENDATION_TYPE" != "upperbound" ]]; then
        echo "N/A,N/A"
        return
    fi
    
    local usage=$(kubectl top pod "$pod" -n "$namespace" --containers=true --no-headers 2>/dev/null | grep "$container_name" | awk '{print $2 "," $3}' 2>/dev/null)
    
    if [[ -z "$usage" || "$usage" == "," ]]; then
        echo "N/A,N/A"
    else
        echo "$usage"
    fi
}

# Function to escape CSV fields (handles commas, quotes, newlines)
escape_csv_field() {
    local field="$1"
    # Replace newlines with spaces, escape quotes, and wrap in quotes if contains comma/quote/newline
    field=$(echo "$field" | tr '\n\r' ' ' | sed 's/"/""/g')
    if [[ "$field" == *","* || "$field" == *'"'* || "$field" == *$'\n'* ]]; then
        echo "\"$field\""
    else
        echo "$field"
    fi
}

# Function to log changes to CSV
log_to_csv() {
    local timestamp="$1"
    local namespace="$2"
    local vpa_name="$3"
    local target_kind="$4"
    local target_name="$5"
    local pod_name="$6"
    local container_name="$7"
    local current_cpu="$8"
    local current_memory="$9"
    
    if [[ "$RECOMMENDATION_TYPE" == "upperbound" ]]; then
        # For upperbound: timestamp,namespace,vpa_name,target_kind,target_name,pod_name,container_name,current_cpu,current_memory,actual_cpu,actual_memory,rec_cpu,rec_memory,rec_memory_k8s,status,error_message
        local actual_cpu="${10}"
        local actual_memory="${11}"
        local rec_cpu="${12}"
        local rec_memory="${13}"
        local status="${14}"
        local error_message="${15}"
        
        # Convert memory to proper K8s format for upperbound
        local rec_memory_k8s="N/A"
        if [[ "$rec_memory" != "N/A" && -n "$rec_memory" ]]; then
            rec_memory_k8s=$(convert_memory_to_k8s_units "$rec_memory")
        fi
        
        # Escape all fields for CSV
        local csv_line="$(escape_csv_field "$timestamp"),$(escape_csv_field "$namespace"),$(escape_csv_field "$vpa_name"),$(escape_csv_field "$target_kind"),$(escape_csv_field "$target_name"),$(escape_csv_field "$pod_name"),$(escape_csv_field "$container_name"),$(escape_csv_field "$current_cpu"),$(escape_csv_field "$current_memory"),$(escape_csv_field "$actual_cpu"),$(escape_csv_field "$actual_memory"),$(escape_csv_field "$rec_cpu"),$(escape_csv_field "$rec_memory"),$(escape_csv_field "$rec_memory_k8s"),$(escape_csv_field "$status"),$(escape_csv_field "$error_message")"
        
        # Use file locking to prevent race conditions when writing to CSV
        (
            flock -x 200
            echo "$csv_line" >> "$CSV_FILE"
        ) 200>>"$CSV_FILE.lock"
    else
        # For target/lowerbound: timestamp,namespace,vpa_name,target_kind,target_name,pod_name,container_name,current_cpu,current_memory,rec_cpu,rec_memory,rec_memory_k8s,status,error_message
        local rec_cpu="${10}"
        local rec_memory="${11}"
        local status="${12}"
        local error_message="${13}"
        
        # Convert memory to proper K8s format
        local rec_memory_k8s="N/A"
        if [[ "$rec_memory" != "N/A" && -n "$rec_memory" ]]; then
            rec_memory_k8s=$(convert_memory_to_k8s_units "$rec_memory")
        fi
        
        # Escape all fields for CSV
        local csv_line="$(escape_csv_field "$timestamp"),$(escape_csv_field "$namespace"),$(escape_csv_field "$vpa_name"),$(escape_csv_field "$target_kind"),$(escape_csv_field "$target_name"),$(escape_csv_field "$pod_name"),$(escape_csv_field "$container_name"),$(escape_csv_field "$current_cpu"),$(escape_csv_field "$current_memory"),$(escape_csv_field "$rec_cpu"),$(escape_csv_field "$rec_memory"),$(escape_csv_field "$rec_memory_k8s"),$(escape_csv_field "$status"),$(escape_csv_field "$error_message")"
        
        # Use file locking to prevent race conditions when writing to CSV
        (
            flock -x 200
            echo "$csv_line" >> "$CSV_FILE"
        ) 200>>"$CSV_FILE.lock"
    fi
}

# Function to convert memory from bytes to standard Kubernetes units
convert_memory_to_k8s_units() {
    local memory_bytes="$1"
    
    # Handle empty or invalid input
    if [[ -z "$memory_bytes" || "$memory_bytes" == "null" || "$memory_bytes" == "N/A" ]]; then
        echo "N/A"
        return
    fi
    
    # Remove quotes if present
    memory_bytes=$(echo "$memory_bytes" | tr -d '"')
    
    # If it's already in K8s format (with suffix), return as-is
    if [[ "$memory_bytes" =~ ^[0-9.]+[KMGT]i?$ ]]; then
        echo "$memory_bytes"
        return
    fi
    
    # Convert bytes to appropriate unit
    if [[ "$memory_bytes" =~ ^[0-9]+$ ]]; then
        local bytes=$memory_bytes
        
        # Convert to most appropriate unit
        if (( bytes >= 1073741824 )); then
            # Convert to Gi (1024^3)
            local gi=$(echo "scale=1; $bytes / 1073741824" | bc -l)
            echo "${gi}Gi"
        elif (( bytes >= 1048576 )); then
            # Convert to Mi (1024^2)  
            local mi=$(echo "scale=0; $bytes / 1048576" | bc -l)
            echo "${mi}Mi"
        elif (( bytes >= 1024 )); then
            # Convert to Ki (1024)
            local ki=$(echo "scale=0; $bytes / 1024" | bc -l)
            echo "${ki}Ki"
        else
            # Keep as bytes
            echo "${bytes}"
        fi
    else
        # Return as-is if we can't parse it
        echo "$memory_bytes"
    fi
}

# Function to convert memory to millicores equivalent for comparison
convert_memory_to_mi() {
    local memory="$1"
    
    # Handle different memory units
    if [[ "$memory" =~ ^([0-9]+)Gi$ ]]; then
        local gb=${BASH_REMATCH[1]}
        echo $(( gb * 1024 ))
    elif [[ "$memory" =~ ^([0-9]+)Mi$ ]]; then
        echo "${BASH_REMATCH[1]}"
    elif [[ "$memory" =~ ^([0-9]+)G$ ]]; then
        local gb=${BASH_REMATCH[1]}
        echo $(( gb * 953 ))  # 1GB ≈ 953Mi
    elif [[ "$memory" =~ ^([0-9]+)M$ ]]; then
        local mb=${BASH_REMATCH[1]}
        echo $(( mb * 95 / 100 ))  # 1MB ≈ 0.95Mi
    elif [[ "$memory" =~ ^([0-9]+)$ ]]; then
        # Assume bytes, convert to Mi
        local bytes=${BASH_REMATCH[1]}
        echo $(( bytes / 1024 / 1024 ))
    else
        echo "0"
    fi
}

# Function to convert CPU to millicores for comparison
convert_cpu_to_millicores() {
    local cpu="$1"
    
    if [[ "$cpu" =~ ^([0-9]+)m$ ]]; then
        echo "${BASH_REMATCH[1]}"
    elif [[ "$cpu" =~ ^([0-9]+)$ ]]; then
        echo $(( ${BASH_REMATCH[1]} * 1000 ))
    elif [[ "$cpu" =~ ^([0-9]*\.?[0-9]+)$ ]]; then
        echo $(( $(echo "${BASH_REMATCH[1]} * 1000" | bc | cut -d. -f1) ))
    else
        echo "0"
    fi
}

get_user_namespaces() {
    local exclude_pattern='^(kube-system|kube-public|kube-node-lease|gke-managed-system|gke-managed-volumepopulator|asm-system|istio-system|istio-operator)$'
    
    if [[ "$EXCLUDE_MY_APP" == "true" ]]; then
        exclude_pattern='^(kube-system|kube-public|kube-node-lease|gke-managed-system|gke-managed-volumepopulator|asm-system|istio-system|istio-operator|my-app)$'
    fi
    
    kubectl get namespaces --no-headers -o custom-columns=":metadata.name" | \
    grep -v -E "$exclude_pattern"
}

# Function to get pods created within specified minutes
get_pods_not_older_than() {
    local namespace="$1"
    local max_age_minutes="$2"
    
    # Get current time in epoch seconds
    local current_epoch=$(date +%s)
    local cutoff_time=$((current_epoch - (max_age_minutes * 60)))
    
    # Get all running pods with their creation timestamps
    kubectl get pods -n "$namespace" --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.creationTimestamp}{"\n"}{end}' 2>/dev/null | while read -r pod_name creation_time; do
        if [[ -n "$pod_name" && -n "$creation_time" ]]; then
            # Convert creation time to epoch
            local creation_epoch=$(date -d "$creation_time" +%s 2>/dev/null)
            
            # Check if pod was created within the specified timeframe
            if [[ -n "$creation_epoch" && $creation_epoch -gt $cutoff_time ]]; then
                echo "$pod_name"
            fi
        fi
    done
}

# Function to check if pod has been healthy for the specified duration
is_pod_eligible() {
    local namespace="$1"
    local pod_name="$2"
    local duration_minutes="$3"
    
    # If no duration specified or duration is 0, all pods are eligible
    if [[ -z "$duration_minutes" || "$duration_minutes" == "0" ]]; then
        return 0
    fi
    
    # Get pod start time and ready condition
    local pod_info=$(kubectl get pod "$pod_name" -n "$namespace" -o json 2>/dev/null)
    
    if [[ -z "$pod_info" ]]; then
        return 1
    fi
    
    # Check if pod is in Running phase
    local phase=$(echo "$pod_info" | jq -r '.status.phase // "Unknown"')
    if [[ "$phase" != "Running" ]]; then
        return 1
    fi
    
    # Get pod start time
    local start_time=$(echo "$pod_info" | jq -r '.status.startTime // empty')
    if [[ -z "$start_time" ]]; then
        return 1
    fi
    
    # Convert start time to epoch
    local start_epoch=$(date -d "$start_time" +%s 2>/dev/null)
    if [[ -z "$start_epoch" ]]; then
        return 1
    fi
    
    # Check if pod has been running for at least the specified duration
    local current_epoch=$(date +%s)
    local running_minutes=$(( (current_epoch - start_epoch) / 60 ))
    
    if [[ $running_minutes -lt $duration_minutes ]]; then
        return 1
    fi
    
    # Check Ready condition - must be True and stable for the duration
    local ready_condition=$(echo "$pod_info" | jq -r '.status.conditions[]? | select(.type=="Ready") | .status // "False"')
    local ready_transition_time=$(echo "$pod_info" | jq -r '.status.conditions[]? | select(.type=="Ready") | .lastTransitionTime // empty')
    
    if [[ "$ready_condition" != "True" || -z "$ready_transition_time" ]]; then
        return 1
    fi
    
    # Convert ready transition time to epoch
    local ready_epoch=$(date -d "$ready_transition_time" +%s 2>/dev/null)
    if [[ -z "$ready_epoch" ]]; then
        return 1
    fi
    
    # Check if pod has been ready for at least the specified duration
    local ready_minutes=$(( (current_epoch - ready_epoch) / 60 ))
    
    if [[ $ready_minutes -lt $duration_minutes ]]; then
        return 1
    fi
    
    return 0
}


# Function to process a single pod (extracted for batch processing)
process_single_pod() {
    local target_namespace="$1"
    local dry_run="$2"
    local pod="$3"
    local vpa_name="$4"
    local target_kind="$5"
    local target_name="$6"
    local container_name="$7"
    local rec_cpu="$8"
    local rec_memory="$9"
    local eligible_pods=("${@:10}")
    
    # Check if pod is eligible (if filtering is enabled)
    if [[ -n "$NOT_OLDER_THAN_MINUTES" || (-n "$DURATION_MINUTES" && "$DURATION_MINUTES" != "0") ]]; then
        local found=false
        for eligible_pod in "${eligible_pods[@]}"; do
            if [[ "$eligible_pod" == "$pod" ]]; then
                found=true
                break
            fi
        done
        if [[ "$found" == "false" ]]; then
            if [[ -n "$NOT_OLDER_THAN_MINUTES" ]]; then
                log_message "      Skipping pod $pod (older than $NOT_OLDER_THAN_MINUTES minutes)"
            else
                log_message "      Skipping pod $pod (not healthy for required duration)"
            fi
            return
        fi
    fi
    
    log_message "      Processing pod: $pod"
    
    # Get current resource requests for the container
    local pod_spec=$(kubectl get pod "$pod" -n "$target_namespace" -o json 2>/dev/null)
    if [[ -z "$pod_spec" ]]; then
        log_message "        ✗ Failed to get pod spec for $pod"
        return
    fi
    
    # Find the container in the pod spec
    local container_spec=$(echo "$pod_spec" | jq -c ".spec.containers[]? | select(.name == \"$container_name\")")
    if [[ -z "$container_spec" ]]; then
        log_message "        ✗ Container $container_name not found in pod $pod"
        return
    fi
    
    # Get current resource requests
    local current_cpu=$(echo "$container_spec" | jq -r '.resources.requests.cpu // "0m"')
    local current_memory=$(echo "$container_spec" | jq -r '.resources.requests.memory // "0Mi"')
    
    log_message "        Current requests - CPU: $current_cpu, Memory: $current_memory"
    
    # Build patch JSON for resource requests
    local patch_resources=()
    
    if [[ "$rec_cpu" != "N/A" ]]; then
        local current_cpu_mc=$(convert_cpu_to_millicores "$current_cpu")
        local target_cpu_mc=$(convert_cpu_to_millicores "$rec_cpu")
        
        if [[ $target_cpu_mc -ne $current_cpu_mc ]]; then
            patch_resources+=("\"cpu\":\"$rec_cpu\"")
            log_message "        Will update CPU: $current_cpu -> $rec_cpu"
        else
            log_message "        CPU already matches $RECOMMENDATION_TYPE recommendation: $rec_cpu"
        fi
    fi
    
    if [[ "$rec_memory" != "N/A" ]]; then
        local current_memory_mi=$(convert_memory_to_mi "$current_memory")
        local target_memory_mi=$(convert_memory_to_mi "$rec_memory")
        
        if [[ $target_memory_mi -ne $current_memory_mi ]]; then
            # Convert memory to proper Kubernetes format
            local k8s_memory
            if [[ "$rec_memory" =~ ^[0-9]+$ ]]; then
                # Pure number (bytes) - convert to Mi
                local memory_mi=$((rec_memory / 1024 / 1024))
                if [[ $memory_mi -eq 0 ]]; then
                    memory_mi=1  # Minimum 1Mi
                fi
                k8s_memory="${memory_mi}Mi"
            else
                # Already has a unit, use as-is
                k8s_memory="$rec_memory"
            fi
            patch_resources+=("\"memory\":\"$k8s_memory\"")
            log_message "        Will update Memory: $current_memory -> $k8s_memory"
        else
            log_message "        Memory already matches $RECOMMENDATION_TYPE recommendation: $rec_memory"
        fi
    fi
    
    if [[ ${#patch_resources[@]} -eq 0 ]]; then
        log_message "        ✓ No changes needed - resources already match recommendations"
        
        local entry_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
        if [[ "$RECOMMENDATION_TYPE" == "upperbound" ]]; then
            local actual_usage=$(get_actual_usage "$target_namespace" "$pod" "$container_name")
            local actual_cpu=$(echo "$actual_usage" | cut -d',' -f1)
            local actual_memory=$(echo "$actual_usage" | cut -d',' -f2)
            log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$actual_cpu" "$actual_memory" "$rec_cpu" "$rec_memory" "No_Change" "Resources already match recommendations"
        else
            log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$rec_cpu" "$rec_memory" "No_Change" "Resources already match recommendations"
        fi
        return
    fi
    
    # Build the complete patch JSON
    if [[ ${#patch_resources[@]} -eq 0 ]]; then
        log_message "        ✗ No valid patch resources generated"
        return
    fi
    
    local requests_content=$(IFS=','; echo "${patch_resources[*]}")
    local patch_json="{\"spec\":{\"containers\":[{\"name\":\"$container_name\",\"resources\":{\"requests\":{$requests_content}}}]}}"
    
    # Validate JSON syntax
    if ! echo "$patch_json" | jq . >/dev/null 2>&1; then
        log_message "        ✗ Generated invalid JSON patch: $patch_json"
        return
    fi
    
    local entry_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    
    if [[ "$dry_run" == "true" ]]; then
        log_message "        [DRY RUN] Would patch pod $pod with: $patch_json"
        
        if [[ "$RECOMMENDATION_TYPE" == "upperbound" ]]; then
            local actual_usage=$(get_actual_usage "$target_namespace" "$pod" "$container_name")
            local actual_cpu=$(echo "$actual_usage" | cut -d',' -f1)
            local actual_memory=$(echo "$actual_usage" | cut -d',' -f2)
            log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$actual_cpu" "$actual_memory" "$rec_cpu" "$rec_memory" "Dry_Run" "Would apply patch"
        else
            log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$rec_cpu" "$rec_memory" "Dry_Run" "Would apply patch"
        fi
    else
        log_message "        Patching pod $pod..."
        
        # Apply the patch using kubectl with subresource resize
        if kubectl patch pod "$pod" -n "$target_namespace" --subresource resize --patch "$patch_json" >/dev/null 2>&1; then
            log_message "        ✓ Successfully updated resource requests for container $container_name in pod $pod"
            
            if [[ "$RECOMMENDATION_TYPE" == "upperbound" ]]; then
                local actual_usage=$(get_actual_usage "$target_namespace" "$pod" "$container_name")
                local actual_cpu=$(echo "$actual_usage" | cut -d',' -f1)
                local actual_memory=$(echo "$actual_usage" | cut -d',' -f2)
                log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$actual_cpu" "$actual_memory" "$rec_cpu" "$rec_memory" "Success" "Resource requests updated successfully"
            else
                log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$rec_cpu" "$rec_memory" "Success" "Resource requests updated successfully"
            fi
        else
            log_message "        ✗ Failed to update resource requests for container $container_name in pod $pod"
            # Get the error for troubleshooting
            local error_msg=$(kubectl patch pod "$pod" -n "$target_namespace" --subresource resize --patch "$patch_json" 2>&1)
            log_message "        Error: $error_msg"
            
            if [[ "$RECOMMENDATION_TYPE" == "upperbound" ]]; then
                local actual_usage=$(get_actual_usage "$target_namespace" "$pod" "$container_name")
                local actual_cpu=$(echo "$actual_usage" | cut -d',' -f1)
                local actual_memory=$(echo "$actual_usage" | cut -d',' -f2)
                log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$actual_cpu" "$actual_memory" "$rec_cpu" "$rec_memory" "Failed" "$error_msg"
            else
                log_to_csv "$entry_timestamp" "$target_namespace" "$vpa_name" "$target_kind" "$target_name" "$pod" "$container_name" "$current_cpu" "$current_memory" "$rec_cpu" "$rec_memory" "Failed" "$error_msg"
            fi
        fi
    fi
}

# Function to process pods in batches (for my-app namespace only)
process_pods_in_batches() {
    local target_namespace="$1"
    local dry_run="$2"
    local vpa_name="$3"
    local target_kind="$4"
    local target_name="$5"
    local container_name="$6"
    local rec_cpu="$7"
    local rec_memory="$8"
    local pods_list="$9"
    local eligible_pods=("${@:10}")
    
    # Convert pods string to array
    local pods_array=()
    while IFS= read -r pod; do
        if [[ -n "$pod" ]]; then
            pods_array+=("$pod")
        fi
    done <<< "$pods_list"
    
    local total_pods=${#pods_array[@]}
    if [[ $total_pods -eq 0 ]]; then
        return
    fi
    
    # Process pods in batches of 10 for my-app namespace
    local batch_size=10
    local batch_count=0
    
    for ((i=0; i<$total_pods; i+=batch_size)); do
        local batch_end=$((i+batch_size-1))
        if [[ $batch_end -ge $total_pods ]]; then
            batch_end=$((total_pods-1))
        fi
        
        batch_count=$((batch_count+1))
        log_message "      Processing batch $batch_count (pods $((i+1))-$((batch_end+1)) of $total_pods)"
        
        # Process batch in parallel
        for ((j=i; j<=batch_end; j++)); do
            if [[ $j -lt $total_pods ]]; then
                process_single_pod "$target_namespace" "$dry_run" "${pods_array[$j]}" "$vpa_name" "$target_kind" "$target_name" "$container_name" "$rec_cpu" "$rec_memory" "${eligible_pods[@]}" &
            fi
        done
        
        # Wait for current batch to complete before starting next batch
        wait
        log_message "      Completed batch $batch_count"
    done
}

# Function to extract VPA recommendations and apply them
process_vpa_recommendations() {
    local target_namespace="$1"
    local dry_run="$2"
    
    # Get eligible pods for this namespace upfront
    local eligible_pods=()
    if [[ -n "$NOT_OLDER_THAN_MINUTES" ]]; then
        log_message "Filtering pods created in the past $NOT_OLDER_THAN_MINUTES minutes..."
        while IFS= read -r pod; do
            if [[ -n "$pod" ]]; then
                eligible_pods+=("$pod")
            fi
        done < <(get_pods_not_older_than "$target_namespace" "$NOT_OLDER_THAN_MINUTES")
        
        log_message "Found ${#eligible_pods[@]} pods not older than $NOT_OLDER_THAN_MINUTES minutes in namespace $target_namespace"
        if [[ ${#eligible_pods[@]} -eq 0 ]]; then
            log_message "No pods found within the specified age limit in namespace $target_namespace, skipping."
            return
        fi
    elif [[ -n "$DURATION_MINUTES" && "$DURATION_MINUTES" != "0" ]]; then
        log_message "Filtering pods that have been healthy for at least $DURATION_MINUTES minutes..."
        while IFS= read -r pod; do
            if is_pod_eligible "$target_namespace" "$pod" "$DURATION_MINUTES"; then
                eligible_pods+=("$pod")
            fi
        done < <(kubectl get pods -n "$target_namespace" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
        
        log_message "Found ${#eligible_pods[@]} eligible pods in namespace $target_namespace"
        if [[ ${#eligible_pods[@]} -eq 0 ]]; then
            log_message "No eligible pods found in namespace $target_namespace, skipping."
            return
        fi
    fi
    
    log_message "=== Processing namespace: $target_namespace ==="
    
    # Find all VPA objects in the namespace
    local vpas=$(kubectl get vpa -n "$target_namespace" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
    
    if [[ -z "$vpas" ]]; then
        log_message "No VPA objects found in namespace $target_namespace"
        return
    fi
    
    # Process each VPA
    echo "$vpas" | while read -r vpa_name; do
        if [[ -z "$vpa_name" ]]; then
            continue
        fi
        
        log_message "Processing VPA: $vpa_name"
        
        # Get VPA details
        local vpa_json=$(kubectl get vpa "$vpa_name" -n "$target_namespace" -o json 2>/dev/null)
        
        if [[ -z "$vpa_json" ]]; then
            log_message "  ✗ Failed to get VPA details for $vpa_name"
            continue
        fi
        
        # Extract target reference
        local target_kind=$(echo "$vpa_json" | jq -r '.spec.targetRef.kind // "N/A"')
        local target_name=$(echo "$vpa_json" | jq -r '.spec.targetRef.name // "N/A"')
        
        log_message "  Target: $target_kind/$target_name"
        
        # Extract container recommendations
        local container_recommendations=$(echo "$vpa_json" | jq -c ".status.recommendation.containerRecommendations // []")
        
        if [[ "$container_recommendations" == "[]" || -z "$container_recommendations" ]]; then
            log_message "  ✗ No container recommendations found for VPA $vpa_name"
            continue
        fi
        
        # Process each container recommendation
        echo "$container_recommendations" | jq -c '.[]' | while read -r container_rec; do
            local container_name=$(echo "$container_rec" | jq -r '.containerName // "N/A"')
            
            # Extract recommendation based on type
            local rec_cpu="N/A"
            local rec_memory="N/A"
            
            case "$RECOMMENDATION_TYPE" in
                "target")
                    rec_cpu=$(echo "$container_rec" | jq -r '.target.cpu // "N/A"')
                    rec_memory=$(echo "$container_rec" | jq -r '.target.memory // "N/A"')
                    ;;
                "upperbound")
                    rec_cpu=$(echo "$container_rec" | jq -r '.upperBound.cpu // "N/A"')
                    rec_memory=$(echo "$container_rec" | jq -r '.upperBound.memory // "N/A"')
                    ;;
                "lowerbound")
                    rec_cpu=$(echo "$container_rec" | jq -r '.lowerBound.cpu // "N/A"')
                    rec_memory=$(echo "$container_rec" | jq -r '.lowerBound.memory // "N/A"')
                    ;;
            esac
            
            log_message "    Container: $container_name"
            log_message "      ${RECOMMENDATION_TYPE^} CPU: $rec_cpu, Memory: $rec_memory"
            
            if [[ "$rec_cpu" == "N/A" && "$rec_memory" == "N/A" ]]; then
                log_message "      ✗ No $RECOMMENDATION_TYPE recommendations available for container $container_name"
                continue
            fi
            
            # Find pods for this workload
            local pods=""
            case "$target_kind" in
                "Deployment")
                    # Try multiple label selector strategies
                    pods=$(kubectl get pods -n "$target_namespace" -l app="$target_name" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                    if [[ -z "$pods" ]]; then
                        # Try alternative label selectors
                        pods=$(kubectl get pods -n "$target_namespace" -l app.kubernetes.io/name="$target_name" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                    fi
                    if [[ -z "$pods" ]]; then
                        # Get deployment selector and use it directly
                        local selector=$(kubectl get deployment "$target_name" -n "$target_namespace" -o jsonpath='{.spec.selector.matchLabels}' 2>/dev/null)
                        if [[ -n "$selector" && "$selector" != "{}" ]]; then
                            # Convert JSON to kubectl label selector format
                            local label_selector=$(echo "$selector" | jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")' 2>/dev/null)
                            if [[ -n "$label_selector" ]]; then
                                pods=$(kubectl get pods -n "$target_namespace" -l "$label_selector" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                            fi
                        fi
                    fi
                    ;;
                "StatefulSet")
                    # Try multiple selector strategies for StatefulSets
                    pods=$(kubectl get pods -n "$target_namespace" -l app="$target_name" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                    if [[ -z "$pods" ]]; then
                        # Try StatefulSet specific selector
                        pods=$(kubectl get pods -n "$target_namespace" -l statefulset.kubernetes.io/pod-name --no-headers -o custom-columns=":metadata.name" | grep "^$target_name-" 2>/dev/null)
                    fi
                    if [[ -z "$pods" ]]; then
                        # Get StatefulSet selector and use it directly
                        local selector=$(kubectl get statefulset "$target_name" -n "$target_namespace" -o jsonpath='{.spec.selector.matchLabels}' 2>/dev/null)
                        if [[ -n "$selector" && "$selector" != "{}" ]]; then
                            local label_selector=$(echo "$selector" | jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")' 2>/dev/null)
                            if [[ -n "$label_selector" ]]; then
                                pods=$(kubectl get pods -n "$target_namespace" -l "$label_selector" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                            fi
                        fi
                    fi
                    ;;
                "DaemonSet")
                    # Try multiple selector strategies for DaemonSets
                    pods=$(kubectl get pods -n "$target_namespace" -l app="$target_name" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                    if [[ -z "$pods" ]]; then
                        # Get DaemonSet selector and use it directly
                        local selector=$(kubectl get daemonset "$target_name" -n "$target_namespace" -o jsonpath='{.spec.selector.matchLabels}' 2>/dev/null)
                        if [[ -n "$selector" && "$selector" != "{}" ]]; then
                            local label_selector=$(echo "$selector" | jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")' 2>/dev/null)
                            if [[ -n "$label_selector" ]]; then
                                pods=$(kubectl get pods -n "$target_namespace" -l "$label_selector" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)
                            fi
                        fi
                    fi
                    ;;
            esac
            
            if [[ -z "$pods" ]]; then
                log_message "      ✗ No pods found for $target_kind/$target_name"
                
                # Additional diagnostics for troubleshooting
                local workload_exists=$(kubectl get "$target_kind" "$target_name" -n "$target_namespace" --no-headers 2>/dev/null | wc -l)
                if [[ $workload_exists -eq 0 ]]; then
                    log_message "      → $target_kind/$target_name does not exist in namespace $target_namespace"
                else
                    local replica_count=$(kubectl get "$target_kind" "$target_name" -n "$target_namespace" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "unknown")
                    local ready_replicas=$(kubectl get "$target_kind" "$target_name" -n "$target_namespace" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
                    log_message "      → $target_kind exists: replicas=$replica_count, ready=$ready_replicas"
                    
                    # Show actual selector for debugging
                    local actual_selector=$(kubectl get "$target_kind" "$target_name" -n "$target_namespace" -o jsonpath='{.spec.selector.matchLabels}' 2>/dev/null)
                    if [[ -n "$actual_selector" && "$actual_selector" != "{}" ]]; then
                        log_message "      → Selector labels: $actual_selector"
                    fi
                fi
                continue
            fi
            
            # Process pods - use batch processing for my-app namespace, sequential for others
            if [[ "$target_namespace" == "my-app" ]]; then
                log_message "      Using batch processing for my-app namespace (batches of 10 pods)"
                process_pods_in_batches "$target_namespace" "$dry_run" "$vpa_name" "$target_kind" "$target_name" "$container_name" "$rec_cpu" "$rec_memory" "$pods" "${eligible_pods[@]}"
            else
                # Sequential processing for other namespaces
                echo "$pods" | while read -r pod; do
                    if [[ -z "$pod" ]]; then
                        continue
                    fi
                    
                    process_single_pod "$target_namespace" "$dry_run" "$pod" "$vpa_name" "$target_kind" "$target_name" "$container_name" "$rec_cpu" "$rec_memory" "${eligible_pods[@]}"
                done
            fi
        done
    done
    
    log_message "=== Completed processing namespace: $target_namespace ==="
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --target)
      RECOMMENDATION_TYPE="target"
      shift
      ;;
    --upperbound)
      RECOMMENDATION_TYPE="upperbound"
      shift
      ;;
    --lowerbound)
      RECOMMENDATION_TYPE="lowerbound"
      shift
      ;;
    --all-namespaces)
      ALL_NAMESPACES=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --pod-healthy-duration)
      # To disable duration filtering, use --pod-healthy-duration 0
      if [[ -n "$2" && "$2" =~ ^[0-9]+$ ]]; then
        DURATION_MINUTES="$2"
        shift 2
      else
        echo "Error: --pod-healthy-duration requires a numeric value (minutes)"
        echo "Usage: $0 [--target|--upperbound|--lowerbound] [namespace] [--all-namespaces] [--dry-run] [--pod-healthy-duration MINUTES] [--exclude-my-app] [--not-older-than MINUTES]"
        exit 1
      fi
      ;;
    --exclude-my-app)
      EXCLUDE_MY_APP=true
      shift
      ;;
    --not-older-than)
      if [[ -n "$2" && "$2" =~ ^[0-9]+$ ]]; then
        NOT_OLDER_THAN_MINUTES="$2"
        shift 2
      else
        echo "Error: --not-older-than requires a numeric value (minutes)"
        echo "Usage: $0 [--target|--upperbound|--lowerbound] [namespace] [--all-namespaces] [--dry-run] [--pod-healthy-duration MINUTES] [--exclude-my-app] [--not-older-than MINUTES]"
        exit 1
      fi
      ;;
    -*)
      echo "Unknown option: $1"
      echo "Usage: $0 [--target|--upperbound|--lowerbound] [namespace] [--all-namespaces] [--dry-run] [--pod-healthy-duration MINUTES] [--exclude-my-app] [--not-older-than MINUTES]"
      exit 1
      ;;
    *)
      if [[ -z "$NAMESPACE" && "$ALL_NAMESPACES" == "false" ]]; then
        NAMESPACE="$1"
        shift
      else
        echo "Error: Multiple namespaces specified or unknown argument: $1"
        echo "Usage: $0 [--target|--upperbound|--lowerbound] [namespace] [--all-namespaces] [--dry-run] [--pod-healthy-duration MINUTES] [--exclude-my-app] [--not-older-than MINUTES]"
        exit 1
      fi
      ;;
  esac
done

# Update file names after recommendation type is determined
LOG_FILE="${CLUSTER_NAME}_vpa-${RECOMMENDATION_TYPE}-requests-${TIMESTAMP}.log"
CSV_FILE="${CLUSTER_NAME}_set-requests-from-vpa-${RECOMMENDATION_TYPE}-${TIMESTAMP}.csv"

# Initialize logging
init_csv
log_message "=== VPA $RECOMMENDATION_TYPE Resource Request Setter Started ==="
log_message "Cluster: $CLUSTER_NAME"
log_message "Recommendation Type: $RECOMMENDATION_TYPE"
log_message "Timestamp: $(date)"

if [[ -n "$NOT_OLDER_THAN_MINUTES" ]]; then
    log_message "Age filter: Only processing pods not older than $NOT_OLDER_THAN_MINUTES minutes"
elif [[ -n "$DURATION_MINUTES" && "$DURATION_MINUTES" != "0" ]]; then
    log_message "Duration filter: Only processing pods healthy for at least $DURATION_MINUTES minutes"
fi

if [[ "$ALL_NAMESPACES" == "true" ]]; then
    log_message "Mode: Processing ALL user namespaces"
    if [[ "$DRY_RUN" == "true" ]]; then
        log_message "DRY RUN MODE - No changes will be made"
    fi
    
    # Get list of user namespaces
    namespaces=($(get_user_namespaces))
    
    if [[ ${#namespaces[@]} -eq 0 ]]; then
        log_message "No user namespaces found to process."
        exit 0
    fi
    
    log_message "Found ${#namespaces[@]} user namespaces: ${namespaces[*]}"
    
    if [[ "$EXCLUDE_MY_APP" == "true" ]]; then
        log_message "Note: 'my-app' namespace is excluded from processing"
    fi
    
    # Process each namespace sequentially to avoid CSV race conditions
    # TODO: In future, could implement per-namespace CSV files and merge at end
    for ns in "${namespaces[@]}"; do
        log_message "Starting processing for namespace: $ns"
        process_vpa_recommendations "$ns" "$DRY_RUN"
    done
    
    log_message "Completed processing all namespaces"
    
else
    # Single namespace mode
    if [[ -z "$NAMESPACE" ]]; then
        NAMESPACE="my-app"
    fi
    
    log_message "Mode: Processing single namespace: $NAMESPACE"
    if [[ "$DRY_RUN" == "true" ]]; then
        log_message "DRY RUN MODE - No changes will be made"
    fi
    
    process_vpa_recommendations "$NAMESPACE" "$DRY_RUN"
fi

log_message "=== VPA $RECOMMENDATION_TYPE Resource Request Setter Completed ==="
log_message "Log file: $LOG_FILE"
log_message "CSV file: $CSV_FILE"

# Clean up lock file
if [[ -f "$CSV_FILE.lock" ]]; then
    rm -f "$CSV_FILE.lock"
fi
