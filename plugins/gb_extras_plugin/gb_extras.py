from airflow.plugins_manager import AirflowPlugin
from airflow.models import DagModel, DagTag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import json as utils_json, timezone, yaml
from airflow.utils.state import State
from airflow.www import utils as wwwutils
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.www import utils as wwwutils
from airflow.configuration import AIRFLOW_CONFIG, conf
from airflow.www.decorators import action_logging, gzipped
from urllib.parse import unquote, urljoin, urlsplit
from sqlalchemy.orm import Session, joinedload
from flask import Blueprint
from sqlalchemy import func, and_, literal
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

    @expose('/gb-dag-dependencies')
    @gzipped
    @action_logging
    def list(self):
        """Display DAG dependencies"""

        title = f"GB DAG Dependencies"
        self.dag_id = request.args.get('dag', "")
        arg_search_query = request.args.get("search")
        arg_tags_filter = request.args.getlist('tags')
        arg_states_filter = request.args.get('states')

        if self.dag_id and self.dag_id != "":
            arg_search_query = self.dag_id # Usado para manter o campo search dags preenchido
        
        # cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
        if arg_tags_filter:
            flask_session[FILTER_TAGS_COOKIE] = ','.join(arg_tags_filter)

        tags = self.get_dag_tags(arg_tags_filter)
        states = [state.value for state in State.dag_states]

        # lista dags que o usuário tem acesso
        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        self._calculate_graph(arg_tags_filter, arg_states_filter, filter_dag_ids)
        self.last_refresh = timezone.utcnow()

        dag_states= [state.value for state in State.dag_states]

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
            nodes=self.nodes,
            dag_id=self.dag_id,
            search_query=arg_search_query if arg_search_query else "",
            tags=tags,
            tags_filter=arg_tags_filter,
            states=states,
            states_filter=arg_states_filter,
            paging=wwwutils.generate_pages(
                1, #current_page,
                1, #num_of_pages,
                search=escape(arg_search_query) if arg_search_query else None
            ),
            edges=self.edges,
            last_refresh=self.last_refresh,
            arrange=conf.get("webserver", "dag_orientation"),
            width=request.args.get("width", "100%"),
            height=request.args.get("height", "800"),
        )

    # DagModal View
    @expose('/gb-dag-details')
    @provide_session
    def get_dag(self, session=None):

        dag_id = request.args.get('dag_id')

        # query = session.query(DagRun.dag_id, DagRun.run_id, DagRun.state, DagRun.execution_date)
        # query = session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id)

        # https://github.com/apache/airflow/blob/main/airflow/www/views.py#L1125
        last_runs_subquery = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_date"),
            )
            .group_by(DagRun.dag_id)
            .filter(DagRun.dag_id == dag_id) 
            .subquery("last_runs")
        )

        query = session.query(
            DagRun
            # DagRun.dag_id,
            # DagRun.start_date,
            # DagRun.end_date,
            # DagRun.state,
            # DagRun.execution_date,
            # DagRun.data_interval_start,
            # DagRun.data_interval_end
        ).join(
            last_runs_subquery,
            and_(
                last_runs_subquery.c.dag_id == DagRun.dag_id,
                last_runs_subquery.c.max_execution_date == DagRun.execution_date,
            ),
        )

        dr = query.first()
        # response = {
        #     "dag_id": r.dag_id,
        #     "state": r.state,
        #     "execution_date": r.execution_date.isoformat(),
        #     "start_date": r.start_date.isoformat(),
        #     "end_date": r.end_date.isoformat(),
        #     "data_interval_start": r.data_interval_start.isoformat(),
        #     "data_interval_end": r.data_interval_end.isoformat(),
        # }

        return self.render_template(
            "/gb_dag_summary.html",
            # "/gb_dag_details.html"
            title=f"Título {dag_id}",
            # dag=query.first().dag_model
            # dag=query.first().__dict__
            dag_data=wwwutils.encode_dag_run(dr)
        )


    @provide_session
    def get_dag_tags(self, arg_tags_filter, session=None):
        dagtags = session.query(DagTag.name).distinct(DagTag.name).all()

        tags = [
            {"name": name, "selected": bool(arg_tags_filter and name in arg_tags_filter)}
            for name, in dagtags
        ]
        return tags

    @provide_session
    def get_last_dag_runs(self, dags, session=None):
        subquery = session.query(DagRun.dag_id,
                                 func.max(DagRun.execution_date).label('execmax')) \
            .filter(DagRun.dag_id.in_(dags)).group_by(DagRun.dag_id).subquery()

        query = session.query(DagRun.dag_id, DagRun.run_id, DagRun.state, DagRun.execution_date)

        query = query.join(subquery, and_(subquery.c.dag_id == DagRun.dag_id,
                                          subquery.c.execmax == DagRun.execution_date))
        out = {}
        for dag_id, run_id, state, execution_date in query.all():
            out[dag_id] = {
                   'run_id': run_id,
                   'state': state,
                   'execution_date': execution_date}
        return out

    # @provide_session
    # def get_last_task_instances(self, dags, session=None):
    #     subquery = session.query(DagRun.dag_id,
    #                              func.max(DagRun.execution_date).label('execmax')) \
    #         .filter(DagRun.dag_id.in_(dags)).group_by(DagRun.dag_id).subquery()

    #     query = session.query(DagRun.dag_id, DagRun.run_id, DagRun.state, DagRun.execution_date)

    #     query = query.join(subquery, and_(subquery.c.dag_id == DagRun.dag_id,
    #                                       subquery.c.execmax == DagRun.execution_date))
    #     out = {}
    #     for dag_id, run_id, state, execution_date in query.all():
    #         out[dag_id] = {
    #             'run_id': run_id,
    #             'state': state,
    #             'execution_date': execution_date}
    #     return out


    def _get_dep(self, dag_graph, dag, direction, level):
        dags_dep = dag_graph[dag][direction].copy()
        if level > 0:
            for d in dag_graph[dag][direction]:
                dags_dep.extend(self._get_dep(dag_graph, d, direction, level - 1))
        return dags_dep

    @provide_session
    def _calculate_graph(self, arg_tags_filter, args_states_filter, filter_dag_ids, session=None):
        def check_exit(_dag_graph, _dag):
            if _dag not in _dag_graph:
                _dag_graph[_dag] = {'from': [], 'to': []}
            return _dag_graph

        nodes = []
        edges = []

        dag_depend = SerializedDagModel.get_dag_dependencies()
        dags_states = self.get_last_dag_runs(list(dag_depend.keys()))

        #organiza as dependencias
        dag_graph = {}
        for dag, dependencies in dag_depend.items():
            dag_graph = check_exit(dag_graph, dag)
            for dep in dependencies:
                dag_graph[dag]['from'].append(dep.source)
                dag_graph = check_exit(dag_graph, dep.source)
                dag_graph[dep.source]['to'].append(dag)

        #Obtem lista de dag com base na tag
        dags_query = session.query(DagModel.dag_id).filter(~DagModel.is_subdag, DagModel.is_active)

        if arg_tags_filter:
            dags_query = dags_query.filter(DagModel.tags.any(DagTag.name.in_(arg_tags_filter)))

        # if args_states_filter:
        #     dags_query = dags_query.filter(DagRun.)

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


        for dag, dependencies in dag_graph.items():
            dag_node_id = f"dag:{dag}"
            state = dags_states[dag]['state'] if dag in dags_states else None
            nodes.append(self._node_dict(dag_node_id, dag, f"dag {state}", ))
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

    # def _calculate_graph(self):
    #
    #     def check_exit(_dag_graph, _dag):
    #         if _dag not in _dag_graph:
    #             _dag_graph[_dag] = {'from': [], 'to': []}
    #         return _dag_graph
    #
    #     nodes = []
    #     edges = []
    #
    #     dag_depend = SerializedDagModel.get_dag_dependencies()
    #     dags_states = self.get_last_dag_runs(list(dag_depend.keys()))
    #     # tasks = []
    #     # for dependencies in dag_depend.values():
    #     #     for dep in dependencies:
    #     #         tasks.append(dep.node_id)
    #
    #     for dag, dependencies in dag_depend.items():
    #         dag_node_id = f"dag:{dag}"
    #         state = dags_states[dag]['state'] if dag in dags_states else None
    #         nodes.append(self._node_dict(dag_node_id, dag, f"dag {state}", ))
    #         for dep in dependencies:
    #             #nodes.append(self._node_dict(dep.node_id, dep.dependency_id, dep.dependency_type))
    #             edges.extend(
    #                 [
    #                     #{"u": f"dag:{dep.source}", "v": dep.node_id},
    #                     #{"u": dep.node_id, "v": f"dag:{dep.target}"},
    #                     {"u": f"dag:{dep.source}", "v": f"dag:{dep.target}"}
    #                 ]
    #             )
    #
    #     self.nodes = nodes
    #     self.edges = edges

    @staticmethod
    def _node_dict(node_id, label, node_class):
        return {
            "id": node_id,
            "value": {"label": label, "rx": 5, "ry": 5, "class": node_class},
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


# instantiate MyBaseView
dag_dependencies = GbDagDependenciesView()

# define the path to my_view in the Airflow UI
view_package = {
    # define the menu sub-item name
    "name": "GB DAG Dependencies",
    # define the top-level menu item
    "category": "GB Extras",
    "view": dag_dependencies,
}


# define the plugin class
class GbPlugin(AirflowPlugin):
    # name the plugin
    name = "GB Extras"
    # add the blueprint and appbuilder_views components
    flask_blueprints = [blueprint]
    appbuilder_views = [view_package]
