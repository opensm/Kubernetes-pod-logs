# -*- coding: utf-8 -*-

from kubernetes import client, config, watch
import sys
import signal
from multiprocessing import Process
import os

reload(sys)
sys.setdefaultencoding('utf-8')

ERROR_KEYS = ['Error']
config.load_kube_config(config_file='./config')


class MonitorPod:
    def __init__(self, version):
        self.version = version
        self.w = watch.Watch()

    def get_pods_by_label(self, label, namespace):
        """
        :param label:
        :param namespace:
        :return:
        """
        pods = [x.metadata.name for x in self.version.list_namespaced_pod(
            namespace=namespace,
            label_selector=label
        ).items]
        return pods

    def alert(self, secret, corpid, agentid, party, message):
        """
        :param secret:
        :param corpid:
        :param agentid:
        :param party:
        :param message:
        :return:
        """
        import requests
        import json
        url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={}&corpsecret={}'
        try:
            getr = requests.get(url=url.format(corpid, secret))
            access_token = getr.json().get('access_token')
        except Exception as error:
            print("获取token失败，{}".format(error))
            sys.exit(1)
        data = {
            "toparty": party,  # 向这些部门发送
            "msgtype": "text",
            "agentid": agentid,  # 应用的 id 号
            "text": {
                "content": message
            }
        }
        try:
            r = requests.post(
                url="https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(access_token),
                data=json.dumps(data)
            )
            print("发送消息成功:{}".format(r.json()))
            return True
        except Exception as error:
            print("发送消息失败,{}".format(error))
            return False


def run_alert_logs(pod, namespace):
    """
    :return:
    """
    k = MonitorPod(version=client.CoreV1Api())
    try:
        for e in k.w.stream(
                k.version.read_namespaced_pod_log, namespace=namespace, name=pod,
        ):
            for log in ERROR_KEYS:
                if log not in e:
                    continue
                message = "{}:日志异常:{}！".format(pod,e)
                print(message)
                # if not k.alert(
                #         message=message,
                #         corpid=1000030,
                #         agentid="aaaa",
                #         party="aaaa",
                #         secret="4KMlUU6brl7DBUoHM0YBAKRU6yexoVh9Be8WeT69h44"
                # ):
                #     os.kill(os.getgid(), signal.SIGINT)

    except Exception as error:
        print("进程退出：{}".format(error))
        os.kill(os.getgid(), signal.SIGINT)


class SubProcessManager:
    def __init__(self):
        self.k = MonitorPod(version=client.CoreV1Api())
        self.process = []

        if not ERROR_KEYS:
            raise ValueError("请输入正常的数据：ERROR_KEYS")

    def run_process(self, label, namespace):
        """
        :return:
        """
        pods = self.k.get_pods_by_label(label=label, namespace=namespace)
        if not pods:
            print("获取pods失败")
            sys.exit()
        for x in pods:
            p = Process(target=run_alert_logs, args=(x, namespace))
            p.start()
            self.process.append(p)
        for x in self.process:
            x.join()
        print("子进程全部退出，即将重启！")
        self.run_process(label=label, namespace=namespace)


if __name__ == '__main__':
    s = SubProcessManager()
    s.run_process(label="app.kubernetes.io/name=ingress-nginx", namespace="ingress-nginx")
