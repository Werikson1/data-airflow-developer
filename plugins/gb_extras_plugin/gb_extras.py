from airflow.configuration import AIRFLOW_CONFIG, conf
from airflow.plugins_manager import AirflowPlugin
from airflow.models import DagModel, DagTag, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import json as utils_json, timezone, yaml
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.www import utils as wwwutils
from airflow.www.decorators import action_logging, gzipped
from urllib.parse import unquote, urljoin, urlsplit
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, and_, literal, desc
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import (
    Markup,
    Response,
    abort,
    before_render_template,
    current_app,
    escape,
    flash,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session as flask_session,
    url_for,
)
import flask.json
import datetime
from datetime import timedelta

def datetime_to_utc(value: datetime) -> str:
    if value and value != None:
        return value.strftime('%Y-%m-%d, %H:%M:%S %Z')
    return ''

def timedelta_to_str(value: timedelta) -> str:
    if value:
        return '{}h {}m {}s'.format(value.seconds//(60*60), value.seconds//60, value.seconds%60)
    return ''

# define a Flask blueprint
blueprint = Blueprint(
    "gb_extras",
    __name__,
    # register airflow/plugins/templates as a Jinja template folder
    template_folder="templates",
    static_folder='static',
    static_url_path="/static/gb_extras"
)

FILTER_TAGS_COOKIE = 'tags_filter'

class GbDagDependenciesView(AppBuilderBaseView):
    # refresh_interval = timedelta(
    #     seconds=conf.getint(
    #         "webserver",
    #         "dag_dependencies_refresh_interval",
    #         fallback=conf.getint("scheduler", "dag_dir_list_interval"),
    #     )
    # )
    # last_refresh = timezone.utcnow() - refresh_interval
    last_refresh = timezone.utcnow()
    nodes = []
    edges = []
    dag_id = ""
    owner_filter = None
    tags_filter = None
    states_filter = None
    arg_search_query = None

    @expose('/gb-dag-dependencies')
    @gzipped
    @action_logging
    def list(self):
        """Display DAG dependencies"""

        title = f"Linhagem"
        self.dag_id = request.args.get('dag', "") # string # campo dag_query opção dag_id
        self.owner_filter = request.args.get("search") # string # campo dag_query opção owner
        self.tags_filter = request.args.getlist('tags') # array # campo tags_filter
        self.states_filter = request.args.get('states') # string # campo states_filter

        if self.dag_id and self.dag_id != "":
            self.arg_search_query = self.dag_id

        if self.owner_filter and self.owner_filter != "":
            self.arg_search_query = self.owner_filter
        
        # cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
        if self.tags_filter:
            flask_session[FILTER_TAGS_COOKIE] = ','.join(self.tags_filter)

        # load filter options
        tags_list = self.load_tags()
        states_list = self.load_states()

        # lista dags que o usuário tem acesso
        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        self._calculate_graph(filter_dag_ids)
        self.last_refresh = timezone.utcnow()

        # if not self.nodes or not self.edges:
        #     self._calculate_graph()
        #     self.last_refresh = timezone.utcnow()
        # elif timezone.utcnow() > self.last_refresh + self.refresh_interval:
        #     max_last_updated = SerializedDagModel.get_max_last_updated_datetime()
        #     if max_last_updated is None or max_last_updated > self.last_refresh:
        #         self._calculate_graph()
        #     self.last_refresh = timezone.utcnow()


        return self.render_template(
            "/gb_dag_dependencies.html",
            title=title,
            dag_id=self.dag_id,
            search_query=self.arg_search_query if self.arg_search_query else "",
            tags_list=tags_list,
            tags_filter=self.tags_filter,
            states_list=states_list,
            states_filter=self.states_filter,
            paging=wwwutils.generate_pages(
                1, #current_page,
                1, #num_of_pages,
                search=escape(self.arg_search_query) if self.arg_search_query else None
            ),
            nodes=self.nodes,
            edges=self.edges,
            last_refresh=self.last_refresh,
            arrange=conf.get("webserver", "dag_orientation"),
            width=request.args.get("width", "100%"),
            height=request.args.get("height", "800"),
        )

    @provide_session
    def load_tags(self, session=None) -> list:
        dagtags = session.query(DagTag.name).distinct(DagTag.name).all()

        tags = [
            {"name": name, "selected": bool(self.tags_filter and name in self.tags_filter)}
            for name, in dagtags
        ]
        return tags
    
    @provide_session
    def load_states(self, session=None) -> list:
        return [state.value for state in State.dag_states]

    @provide_session
    def get_last_dag_runs(self, dags, session=None) -> dict:
        subquery = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label('execmax')
            )
            .filter(DagRun.dag_id.in_(dags))
            .group_by(DagRun.dag_id)
            .subquery()
        )

        query = (
            session.query(
                DagRun.dag_id, DagRun.run_id, DagRun.state, DagRun.execution_date
            )
            .join(subquery, and_(
                subquery.c.dag_id == DagRun.dag_id,
                subquery.c.execmax == DagRun.execution_date)
            )
        )

        out = {}
        for dag_id, run_id, state, execution_date in query.all():
            out[dag_id] = {
                   'run_id': run_id,
                   'state': state,
                   'execution_date': execution_date}
        return out

    def _get_dep(self, dag_graph, dag, direction, level) -> list:
        dags_dep = dag_graph[dag][direction].copy()
        if level > 0:
            for d in dag_graph[dag][direction]:
                dags_dep.extend(self._get_dep(dag_graph, d, direction, level - 1))
        return dags_dep

    @provide_session
    def _calculate_graph(self, filter_dag_ids, session=None):
        def check_exit(_dag_graph, _dag):
            if _dag not in _dag_graph:
                _dag_graph[_dag] = {'from': [], 'to': []}
            return _dag_graph

        nodes = []
        edges = []

        dag_depend = SerializedDagModel.get_dag_dependencies()

        #organiza as dependencias
        dag_graph = {}
        for dag, dependencies in dag_depend.items():
            dag_graph = check_exit(dag_graph, dag)
            for dep in dependencies:
                dag_graph[dag]['from'].append(dep.source)
                dag_graph = check_exit(dag_graph, dep.source)
                dag_graph[dep.source]['to'].append(dag)

        # Filtro de dag ativa
        dags_query = session.query(DagModel.dag_id).filter(~DagModel.is_subdag, DagModel.is_active)

        # Filtro de tags
        if self.tags_filter:
            dags_query = dags_query.filter(DagModel.tags.any(DagTag.name.in_(self.tags_filter)))

        # if args_states_filter:
        #     dags_query = dags_query.filter(DagRun.)

        # Filtro do owner
        if self.owner_filter:
            dags_query = dags_query.filter(DagModel.owners.like(self.owner_filter))

        # Filtro de dags permitidas para o usuário
        dags_query = dags_query.filter(DagModel.dag_id.in_(filter_dag_ids))

        filtered_dags = (
            dags_query.order_by(DagModel.dag_id)
            #.options(joinedload(DagModel.tags))
            #.limit(dags_per_page)
            .all()
        )


        # filtrar 2 niveis para cada lado da dag apenas dag
        if self.dag_id != "":
            dags = [self.dag_id]
            dags.extend(self._get_dep(dag_graph, self.dag_id, 'to', 1))
            dags.extend(self._get_dep(dag_graph, self.dag_id, 'from', 1))
        else:
            dags = []
            for filtered_dag in filtered_dags:
                dags.append(filtered_dag[0])
                dags.extend(self._get_dep(dag_graph, filtered_dag[0], 'to', 1))
                dags.extend(self._get_dep(dag_graph, filtered_dag[0], 'from', 1))

        remover = []
        #todo é possivel apenas colocar in if no for abaixo depois que for tratado o carregamento sem dag_id
        for dag in dag_graph.keys():
            if dag not in dags:
                remover.append(dag)
        for dag in remover:
            del dag_graph[dag]


        # Verifica o preenchimento dos filtros para mostrar os status das dag
        dags_states = []
        if not all(item is None or item == '' or item == [] for item in [
            self.tags_filter, 
            self.states_filter, 
            self.owner_filter, 
            self.dag_id
        ]):
            dags_states = self.get_last_dag_runs(list(dag_depend.keys()))

        for dag, dependencies in dag_graph.items():
            dag_node_id = f"dag:{dag}"
            state = dags_states[dag]['state'] if dag in dags_states else ''
            execution_date = dags_states[dag]['execution_date'].strftime("%Y-%m-%d %H:%M %Z") if dag in dags_states else ''
            nodes.append(self._node_dict(
                node_id = dag_node_id, 
                label = "",
                node_class = f"dag {state}",
                dag = dag,
                execution_date = execution_date,
                state = state
                )
            )
            for dep in dependencies["from"]:
                if dep in dag_graph:
                    #garante que exibe apenas as dags selecionadas
                    edges.extend(
                        [
                            {"u": f"dag:{dep}", "v": f"dag:{dag}"}
                        ]
                    )

        self.nodes = nodes
        self.edges = edges

    @staticmethod
    def _node_dict(node_id, label, node_class, **kwargs):
        if 'dag' in kwargs:
            label += f'<p>{kwargs["dag"]}</p>'
        if 'execution_date' in kwargs:
            label += f'<p class="date">Last Run: {kwargs["execution_date"]}</p>'
        if 'state' in kwargs:
            label += f'<p class="state {kwargs["state"]}">{kwargs["state"]}</p>'
        return {
            'id': node_id,
            'value': {'label': label, 'rx': 5, 'ry': 5, 'padding': 0, 'class': node_class, 'labelType': 'html'},
        }
    

    # Search Dags Autocomplete Code
    # Source
    # https://github.com/apache/airflow/blob/main/airflow/www/views.py#L5588
    # https://github.com/apache/airflow/blob/main/airflow/www/templates/airflow/dags.html#L170
    # https://github.com/apache/airflow/blob/main/airflow/www/static/js/dags.js#L124
    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    @provide_session
    @expose("/gb-dag-dependencies/autocomplete")
    def autocomplete(self, session=None):
        """Autocomplete."""
        query = unquote(request.args.get("query", ""))

        if not query:
            return flask.json.jsonify([])

        # Provide suggestions of dag_ids and owners
        dag_ids_query = session.query(
            literal("dag").label("type"),
            DagModel.dag_id.label("name"),
        ).filter(~DagModel.is_subdag, DagModel.is_active, DagModel.dag_id.ilike("%" + query + "%"))

        owners_query = (
            session.query(
                literal("owner").label("type"),
                DagModel.owners.label("name"),
            )
            .distinct()
            .filter(~DagModel.is_subdag, DagModel.is_active, DagModel.owners.ilike("%" + query + "%"))
        )

        # # Hide DAGs if not showing status: "all"
        # status = flask_session.get(FILTER_STATUS_COOKIE)
        # if status == "active":
        #     dag_ids_query = dag_ids_query.filter(~DagModel.is_paused)
        #     owners_query = owners_query.filter(~DagModel.is_paused)
        # elif status == "paused":
        #     dag_ids_query = dag_ids_query.filter(DagModel.is_paused)
        #     owners_query = owners_query.filter(DagModel.is_paused)

        # filter_dag_ids = get_airflow_app().appbuilder.sm.get_accessible_dag_ids(g.user)

        # dag_ids_query = dag_ids_query.filter(DagModel.dag_id.in_(filter_dag_ids))
        # owners_query = owners_query.filter(DagModel.dag_id.in_(filter_dag_ids))

        payload = [
            row._asdict() for row in dag_ids_query.union(owners_query).order_by("name").limit(10).all()
        ]
        return flask.json.jsonify(payload)
    

    # DagModal View
    # https://github.com/apache/airflow/blob/main/airflow/www/views.py#L270
    @expose('/gb-dag-summary')
    @provide_session
    def get_dag(self, session=None):

        dag_id = request.args.get('dag_id')

        # (subconsulta) Consulta as tags da dag apresentada na modal
        subquery_tag = (
            session.query(
                DagTag.dag_id,
                func.string_agg(DagTag.name, ', ').label("tags")
            )
            .group_by(DagTag.dag_id)
            .subquery('tags')
        )

        # (subconsulta) Consulta as execuções da dag apresentada na modal
        subquery_dagrun = (
            session.query(
                DagRun.dag_id,
                DagRun.execution_date,
                DagRun.start_date,
                DagRun.end_date,
                DagRun.state,
                func.row_number().over(partition_by=DagRun.dag_id, order_by=desc(DagRun.start_date)).label('rn')
            )
            .subquery('recent_runs')
        )

        # (subconsulta) Consulta os indicadores de execução da dag apresentada na modal
        subquery_recent_dagrun = (
            session.query(
                subquery_dagrun.c.dag_id,
                func.count(subquery_dagrun.c.execution_date).label('count_run'),
                func.max(subquery_dagrun.c.execution_date).label("last_run_date"),
                func.min(subquery_dagrun.c.end_date - subquery_dagrun.c.start_date).label("min_run_duration"),
                func.avg(subquery_dagrun.c.end_date - subquery_dagrun.c.start_date).label("avg_run_duration"),
                func.max(subquery_dagrun.c.end_date - subquery_dagrun.c.start_date).label("max_run_duration")
            )
            .select_from(
                subquery_dagrun
            )
            .filter(subquery_dagrun.c.rn <= 15)
            .group_by(subquery_dagrun.c.dag_id)
            .subquery('runs')
        )

        # (subconsulta) Consulta as tasks id e operadores utilizados da dag apresentada na modal
        subquery_taskinstance = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.operator,
                func.row_number().over(partition_by=and_(TaskInstance.dag_id, TaskInstance.task_id), order_by=desc(TaskInstance.run_id)).label('rn')
            )
            .subquery('recent_instance')
        )

        # (subconsulta) Consulta a quantidade de tasks da dag apresentada na modal
        subquery_recent_taskinstance = (
            session.query(
                subquery_taskinstance.c.dag_id,
                func.count(subquery_taskinstance.c.task_id).label('total_tasks')
            )
            .select_from(
                subquery_taskinstance
            )
            .filter(subquery_taskinstance.c.rn == 1)
            .group_by(subquery_taskinstance.c.dag_id)
            .subquery('instance')
        )

        # Consulta as informações da dag e faz o join com as subconsultas
        query = (
            session.query(
                DagModel.dag_id,
                DagModel.owners,
                subquery_tag.c.tags,
                DagModel.schedule_interval,
                DagModel.timetable_description,
                DagModel.next_dagrun,
                DagModel.next_dagrun_data_interval_start,
                DagModel.next_dagrun_data_interval_end,
                subquery_recent_dagrun.c.count_run,
                subquery_recent_dagrun.c.last_run_date,
                subquery_recent_dagrun.c.min_run_duration,
                subquery_recent_dagrun.c.avg_run_duration,
                subquery_recent_dagrun.c.max_run_duration,
                subquery_recent_taskinstance.c.total_tasks
            )
            .outerjoin(
                subquery_tag,
                and_(
                     subquery_tag.c.dag_id == DagModel.dag_id
                )
            )
            .outerjoin(
                subquery_recent_dagrun,
                and_(
                    subquery_recent_dagrun.c.dag_id == DagModel.dag_id
                )
            )
            .outerjoin(
                subquery_recent_taskinstance,
                and_(
                    subquery_recent_taskinstance.c.dag_id == DagModel.dag_id
                )
            )
            .filter(DagModel.dag_id == dag_id)
        )

        data = query.first()

        # Tratamento do response para apresentar no template da modal
        reponse = {
            'dag_id': data.dag_id,
            'owners': data.owners,
            'tags': data.tags,
            'schedule_interval': data.schedule_interval,
            'timetable_description': data.timetable_description,
            'count_run': 0 if data.count_run is None else data.count_run,
            'next_dagrun': datetime_to_utc(data.next_dagrun),
            'next_dagrun_data_interval_start': datetime_to_utc(data.next_dagrun_data_interval_start),
            'next_dagrun_data_interval_end': datetime_to_utc(data.next_dagrun_data_interval_end),
            'last_run_date': datetime_to_utc(data.last_run_date),
            'min_run_duration': timedelta_to_str(data.min_run_duration),
            'avg_run_duration': timedelta_to_str(data.avg_run_duration),
            'max_run_duration': timedelta_to_str(data.max_run_duration),
            'total_tasks': 0 if data.total_tasks is None else data.total_tasks
        }

        # Consulta os operadores utilizados na dag apresentada na modal
        query_operator = (
            session.query(
                subquery_taskinstance.c.dag_id,
                subquery_taskinstance.c.operator,
                func.count(subquery_taskinstance.c.operator).label('total_operator')
            )
            .select_from(
                subquery_taskinstance
            )
            .filter(and_(subquery_taskinstance.c.rn == 1, subquery_taskinstance.c.dag_id == dag_id))
            .group_by(subquery_taskinstance.c.dag_id, subquery_taskinstance.c.operator)
            .order_by(subquery_taskinstance.c.operator)
        )

        data_operator = query_operator.all()

        return self.render_template(
            "/gb_dag_summary.html",
            title=f"Título {dag_id}",
            dag_summary=reponse,
            operator_summary=data_operator
        )


# instantiate MyBaseView
dag_dependencies = GbDagDependenciesView()

# define the path to my_view in the Airflow UI
view_package = {
    # define the menu sub-item name
    "name": "Linhagem",
    # define the top-level menu item
    "category": "Grupo Boticário",
    "view": dag_dependencies,
}


# define the plugin class
class GbPlugin(AirflowPlugin):
    # name the plugin
    name = "Grupo Boticário"
    # add the blueprint and appbuilder_views components
    flask_blueprints = [blueprint]
    appbuilder_views = [view_package]
