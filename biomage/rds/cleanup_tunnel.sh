tmp_socket_prefix=/tmp/tmp-tunnel

ssh -O exit -S $tmp_socket_prefix-ssh.sock *
rm -f $tmp_socket_prefix*
echo "Finished cleaning up"