#!/bin/sh

send_telegram_msg() {
	chat_id=$1
	message="$2"
	api_key=$(cat /home/mkoterev/.tg_api_key)
	curl -s -X POST "https://api.telegram.org/bot$api_key/sendMessage" \
        -d chat_id=$chat_id \
        -d text="$message" >/dev/null
}

disk="/dev/vda2" 
disk_usage=$(df | grep -E "^$disk" | awk '{print $5}' | tr -d %)

threshold=90
date=$(date -Iminutes)

if [ $disk_usage -gt $threshold ]; then
	message="Disk usage is more than $threshold ($disk_usage%)"
	echo "$date - $message"

	karpov_chat_id=396212094
	koterev_chat_id=444402885
	send_telegram_msg $koterev_chat_id "$message"
	send_telegram_msg $karpov_chat_id "$message"
else
	echo "$date - Disk usage is OK - $disk_usage"
fi
