const workersView = document.getElementById('workers');
const pendingView = document.getElementById('pending');
const connectionError = document.getElementById('connection-error');
const actionMessage = document.getElementById('action-message');
const form = document.getElementById('create-form');
const packageInput = document.getElementById('package-file');
const clearPackage = document.getElementById('clear-package');
const statefulInput = document.getElementById('stateful');
const partitioningInput = document.getElementById('partitioning');
const partitionsInput = document.getElementById('partitions');
const replicasInput = document.getElementById('replicas');
const statefulView = document.getElementById('stateful-services');
const statefulList = document.getElementById('stateful-list');
const stoppingInstances = new Map();
const workerActions = new Map();
let refreshing = false;
let previousView = '';
let statelessReplicas = replicasInput.value;
document.getElementById('coordinator').textContent = location.host;

function element(tag, className, text) {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function badge(status) {
  const node = element('span', 'badge', status === 'unregistered' ? 'Not registered' : status);
  node.dataset.status = status;
  return node;
}

async function request(path, options) {
  const response = await fetch(path, options);
  if (response.status === 204) return;
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}

function message(text, failed = false) {
  actionMessage.textContent = text;
  actionMessage.dataset.error = String(failed);
}

function instanceCard(service, instance) {
  const card = element('article', 'instance');
  const heading = element('div', 'row');
  const title = element('div', 'instance-title');
  title.append(element('h3', 'instance-name', service.name));
  if (service.stateful) title.append(element('span', 'service-kind', 'Stateful'));
  heading.append(title, badge(instance.status));
  card.append(heading, element('p', 'instance-id', instance.id));
  if (service.endpoint) card.append(serviceLink(service));
  const values = element('dl', 'values');
  for (const [label, value] of [['PID', instance.pid || '—'], ['Port', instance.port || '—'], ['Restarts', instance.restarts]]) {
    const item = element('div', '');
    item.append(element('dt', '', label), element('dd', '', value));
    values.append(item);
  }
  card.append(values);
  if (instance.error) card.append(element('p', 'error', instance.error));
  if (stoppingInstances.has(instance.id) && stoppingInstances.get(instance.id) !== instance.pid) {
    stoppingInstances.delete(instance.id);
  }
  const stopping = stoppingInstances.has(instance.id);
  const stopButton = element('button', 'stop', stopping ? 'Stopping…' : 'Stop instance');
  stopButton.type = 'button';
  stopButton.dataset.instance = instance.id;
  stopButton.dataset.action = 'stop';
  stopButton.disabled = stopping || instance.status !== 'running' || !instance.pid;
  stopButton.setAttribute('aria-label', `Stop instance ${instance.id} of ${service.name}`);
  stopButton.title = 'Stops this process; its worker will start a replacement';
  stopButton.addEventListener('click', async () => {
    stoppingInstances.set(instance.id, instance.pid);
    stopButton.disabled = true;
    stopButton.textContent = 'Stopping…';
    try {
      await request(`/services/${encodeURIComponent(service.name)}/instances/${encodeURIComponent(instance.id)}/stop`, { method: 'POST' });
      message(`Stop requested for ${service.name}. Its worker will restart this instance.`);
      await refresh();
    } catch (error) {
      stoppingInstances.delete(instance.id);
      message(error.message, true);
      stopButton.disabled = false;
      stopButton.textContent = 'Stop instance';
    }
  });
  const button = element('button', 'delete', 'Delete service');
  button.type = 'button';
  button.dataset.service = service.name;
  button.dataset.instance = instance.id;
  button.dataset.action = 'delete';
  button.setAttribute('aria-label', `Delete service ${service.name}`);
  button.title = 'Stops all instances of this service';
  button.addEventListener('click', async () => {
    button.disabled = true;
    button.textContent = 'Deleting…';
    try {
      await request(`/services/${encodeURIComponent(service.name)}`, { method: 'DELETE' });
      message(`Deleted ${service.name}. Workers will stop all its instances on their next heartbeat.`);
      await refresh();
    } catch (error) {
      message(error.message, true);
      button.disabled = false;
      button.textContent = 'Delete service';
    }
  });
  const actions = element('div', 'instance-actions');
  actions.append(stopButton, button);
  card.append(actions);
  return card;
}

function serviceLink(service) {
  const link = element('a', 'service-link', service.endpoint);
  link.href = service.endpoint;
  link.target = '_blank';
  link.rel = 'noopener';
  link.title = 'One address for this service, balanced across its available copies';
  return link;
}

function renderStatefulServices(services) {
  statefulList.replaceChildren();
  const statefulServices = services.filter(service => service.stateful);
  statefulView.hidden = !statefulServices.length;
  for (const service of statefulServices) {
    const card = element('article', 'stateful-service');
    const heading = element('div', 'stateful-service-heading');
    heading.append(element('h3', 'instance-name', service.name));
    if (service.endpoint) heading.append(serviceLink(service));
    const count = service.partitions || service.state?.partitions || 1;
    const strategy = service.partitioning || service.state?.partitioning || 'none';
    card.append(heading, element('p', 'muted stateful-description', `${count} ${count === 1 ? 'partition' : 'partitions'} · ${strategy === 'hash' ? 'Hash by key' : 'Single partition'} · 3 copies of the data`));
    const partitions = element('div', 'partitions');
    for (const partition of service.partition_status || []) {
      const section = element('section', 'partition');
      const row = element('div', 'row');
      row.append(element('h4', '', `Partition ${partition.index}`), badge(partition.status));
      section.append(row, element('p', 'partition-leader', `Leader: ${partition.leader_id || 'Not reported'}`));
      const replicas = element('ul', 'state-replicas');
      for (const replica of partition.replicas) {
        const item = element('li', '');
        item.append(element('span', 'replica-worker', replica.worker_id), element('span', 'replica-role', `${replica.role} · ${replica.status}`));
        replicas.append(item);
      }
      section.append(replicas);
      partitions.append(section);
    }
    if (!partitions.childElementCount) partitions.append(element('p', 'empty', 'Waiting for workers to report dictionary status.'));
    card.append(partitions);
    statefulList.append(card);
  }
}

function workerControl(worker) {
  const action = worker.status === 'ready' ? 'stop' : 'start';
  const label = action === 'start' ? 'Start worker' : 'Stop worker';
  const pending = workerActions.get(worker.id);
  const button = element('button', 'stop', pending ? (pending === 'start' ? 'Starting…' : 'Stopping…') : label);
  button.type = 'button';
  button.dataset.workerId = worker.id;
  button.disabled = Boolean(pending);
  button.setAttribute('aria-label', `${label} ${worker.id}`);
  button.title = action === 'stop'
    ? 'Stops this agent and its application processes; keeps the VM running'
    : 'Starts the installed worker service';
  button.addEventListener('click', async () => {
    workerActions.set(worker.id, action);
    button.disabled = true;
    button.textContent = action === 'start' ? 'Starting…' : 'Stopping…';
    try {
      await request(`/workers/${encodeURIComponent(worker.id)}/${action}`, { method: 'POST' });
      message(action === 'start'
        ? `Started the agent on ${worker.id}. Waiting for it to register.`
        : `Stopped the agent and application processes on ${worker.id}. Its VM is still running.`);
    } catch (error) {
      message(error.message, true);
    } finally {
      workerActions.delete(worker.id);
      button.disabled = false;
      button.textContent = label;
      await refresh();
    }
  });
  return button;
}

function render(workers, services) {
  const focusedInstance = document.activeElement.dataset.instance;
  const focusedAction = document.activeElement.dataset.action;
  const focusedWorker = document.activeElement.dataset.workerId;
  workersView.replaceChildren();
  renderStatefulServices(services);
  const pending = pendingView.querySelector('.instances');
  pending.replaceChildren();
  const assigned = new Map(workers.map(worker => [worker.id, []]));
  for (const service of services) {
    for (const instance of service.instances) {
      const card = instanceCard(service, instance);
      if (assigned.has(instance.worker_id)) assigned.get(instance.worker_id).push(card);
      else pending.append(card);
    }
  }
  for (const worker of workers) {
    const card = element('section', 'worker');
    const header = element('div', 'worker-header');
    const row = element('div', 'row');
    row.append(element('h3', 'worker-name', worker.id), badge(worker.status));
    const address = element('div', 'worker-address');
    address.append(element('span', '', worker.ip ? `IP ${worker.ip}` : 'IP unavailable'));
    if (worker.can_start) address.append(workerControl(worker));
    const seen = element('p', 'muted worker-seen');
    seen.dataset.worker = worker.id;
    header.append(row, address, seen);
    const instances = element('div', 'instances');
    const children = assigned.get(worker.id);
    if (children.length) instances.append(...children);
    else instances.append(element('p', 'empty', 'No instances assigned.'));
    card.append(header, instances);
    workersView.append(card);
  }
  if (!workers.length) workersView.append(element('p', 'empty-cluster empty', 'No workers registered. Start a worker agent to see it here.'));
  pendingView.hidden = !pending.childElementCount;
  if (focusedInstance) {
    const button = [...document.querySelectorAll('button[data-instance]')].find(node => node.dataset.instance === focusedInstance && node.dataset.action === focusedAction);
    if (button) button.focus({ preventScroll: true });
  } else if (focusedWorker) {
    const button = [...document.querySelectorAll('button[data-worker-id]')].find(node => node.dataset.workerId === focusedWorker);
    if (button) button.focus({ preventScroll: true });
  }
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const [workerData, serviceData] = await Promise.all([request('/workers'), request('/services')]);
    const workers = workerData.workers;
    const services = serviceData.services;
    const instanceIDs = new Set(services.flatMap(service => service.instances.map(instance => instance.id)));
    for (const id of stoppingInstances.keys()) {
      if (!instanceIDs.has(id)) stoppingInstances.delete(id);
    }
    const view = JSON.stringify({ workers: workers.map(worker => [worker.id, worker.status, worker.ip, worker.can_start, worker.state_address]), workerActions: [...workerActions], services });
    if (view !== previousView) {
      render(workers, services);
      previousView = view;
    }
    for (const label of document.querySelectorAll('[data-worker]')) {
      const worker = workers.find(item => item.id === label.dataset.worker);
      label.textContent = worker.session_id
        ? 'Last seen ' + new Date(worker.last_seen).toLocaleString(undefined, { timeZone: 'UTC', hour12: false }) + ' UTC'
        : 'Agent has not registered.';
    }
    const ready = workers.filter(worker => worker.status === 'ready').length;
    const stateWorkers = new Set(workers.filter(worker => worker.status === 'ready' && worker.state_address).map(worker => worker.state_address)).size;
    document.getElementById('stateful-help').textContent = stateWorkers >= 3
      ? 'A dictionary stored across three workers. Three workers with collections enabled are ready.'
      : `Needs three workers with collections enabled; ${stateWorkers} ${stateWorkers === 1 ? 'is' : 'are'} ready.`;
    const running = services.flatMap(service => service.instances).filter(instance => instance.status === 'running').length;
    document.getElementById('summary').textContent = `${ready} / ${workers.length} ready · ${running} running`;
    document.getElementById('updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
    connectionError.hidden = true;
  } catch (error) {
    connectionError.textContent = 'Could not refresh the cluster: ' + error.message;
    connectionError.hidden = false;
  } finally {
    refreshing = false;
  }
}

packageInput.addEventListener('change', () => {
  clearPackage.hidden = !packageInput.files.length;
});

clearPackage.addEventListener('click', () => {
  packageInput.value = '';
  clearPackage.hidden = true;
});

function updateStateFields() {
  const stateful = statefulInput.checked;
  replicasInput.readOnly = stateful;
  if (stateful) replicasInput.value = '3';
  document.getElementById('replicas-help').textContent = stateful
    ? 'Three application copies, one on each worker.'
    : 'Number of instances to run across the workers.';
  document.getElementById('state-fields').hidden = !stateful;
  partitioningInput.disabled = !stateful;
  const hash = stateful && partitioningInput.value === 'hash';
  document.getElementById('partition-count-field').hidden = !hash;
  partitionsInput.disabled = !hash;
  partitionsInput.required = hash;
}

statefulInput.addEventListener('change', () => {
  if (statefulInput.checked) statelessReplicas = replicasInput.value;
  else replicasInput.value = statelessReplicas;
  updateStateFields();
});
partitioningInput.addEventListener('change', updateStateFields);
updateStateFields();

form.addEventListener('submit', async event => {
  event.preventDefault();
  const button = document.getElementById('create-button');
  const packageFile = packageInput.files[0];
  button.disabled = true;
  button.textContent = packageFile ? 'Uploading…' : 'Creating…';
  const fields = new FormData(form);
  const service = {
    name: fields.get('name'), command: fields.get('command'),
    port: Number(fields.get('port')), replicas: Number(fields.get('replicas')),
  };
  if (statefulInput.checked) {
    service.stateful = true;
    service.partitioning = partitioningInput.value;
    service.partitions = partitioningInput.value === 'hash' ? Number(partitionsInput.value) : 1;
  }
  const args = fields.get('args').split(/\r?\n/).filter(argument => argument.trim() !== '');
  if (args.length) service.args = args;
  try {
    if (packageFile) {
      const uploaded = await request('/packages', {
        method: 'POST', headers: { 'Content-Type': 'application/zip' }, body: packageFile,
      });
      service.package = uploaded.id;
      button.textContent = 'Creating…';
    }
    await request('/services', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(service) });
    message(`Created ${service.name}.`);
    document.getElementById('name').value = '';
    await refresh();
  } catch (error) {
    message(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = 'Create service';
  }
});

async function poll() {
  await refresh();
  setTimeout(poll, 1000);
}
poll();
