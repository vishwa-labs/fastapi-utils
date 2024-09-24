import os.path


def __get_info_from_pod(key_name):
    file_path = f"/etc/podinfo/{key_name}"
    if os.path.exists(file_path):
        file = open(file_path, "r")
        return ''.join(file.readlines())
    return None


def __get_pod_name():
    pod_name = os.getenv("POD_NAME", None) if os.getenv("POD_NAME", None) is not None else __get_info_from_pod("name")
    return pod_name if pod_name is not None else "local"

    
def __get_container_name():
    labels = __get_info_from_pod("labels") if __get_info_from_pod("labels") is not None else "local"
    if not labels == "local":
        container_label = [s for s in labels.split("\n") if "app.kubernetes.io/name=" in s]
        if len(container_label) == 0:
            return labels
        container_name = container_label[0].split("=", 1)[1]
        return container_name[1: len(container_name)-1]
    return labels


def __get_service_version():
    labels = __get_info_from_pod("labels") if __get_info_from_pod("labels") is not None else "local"
    if not labels == "local":
        container_label = [s for s in labels.split("\n") if "app.kubernetes.io/version=" in s]
        if len(container_label) == 0:
            return labels
        container_name = container_label[0].split("=", 1)[1]
        return container_name[1: len(container_name)-1]
    return labels


NAMESPACE = __get_info_from_pod("namespace") if __get_info_from_pod("namespace") is not None else "local"
POD_NAME = __get_pod_name()
CONTAINER_NAME = __get_container_name()
SERVICE_VERSION = __get_service_version()
NODE_IP = os.getenv("node-ip", None) if os.getenv("node-ip", None) is not None else __get_info_from_pod("node-ip")
