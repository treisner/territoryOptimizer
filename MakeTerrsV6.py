import psycopg2
import operator
import networkx as nx
from sklearn.cluster import KMeans,DBSCAN
import numpy as np
import color_graph


def initialize_assignedzips():

    sql=(\
        "delete from assignedzips;"
        "insert into assignedzips (zip,drivetime,miles,targets,zipgroup) "
        "(select zip, "
        "cast(0.0 as double precision) as drivetime, "
        "cast(0.0 as double precision) as miles, "
        "targets ,cast(-1 as integer) as zipgroup "
        "from zipswithtargets where targets>0) "
    )

    crs.execute(sql)
    sql=(\
        "insert into assignedzips (zip,drivetime,miles,targets,zipgroup)"
        "(select zl.zip as zip,"
        "cast(0 as DOUBLE PRECISION) as drivetime, "
        "cast(0 as DOUBLE PRECISION) as miles, "
        "0 as target, -1 as zipgroup "
        "from zipshapes zl left join assignedzips az "
        "on zl.zip=az.zip where az.zip is null)")
    crs.execute(sql)


def find_startingpoints():
    #print("Finding Cluster Centers with max miles to next target: {} and minimum targets in cluster: {}".format(int(eps*69),int(min_samples)),flush=True)
    crs.execute("select count(zipgroup) from currentbases where cityid='None'")
    badgroups=crs.fetchall()[0][0]
    if badgroups==0:
        return
    sql="select a.zip ,zl.x, zl.y, targets from assignedzips a, ziplocations zl where zl.zip=a.zip and targets>0 and zipgroup=-1"
    crs.execute(sql)
    zips=crs.fetchall()
    minvalue=999999999
    for z in zips:
        if z[3]<minvalue:
            minvalue=z[3]
    if minvalue==999999999 or minvalue<1:
        minvalue=1
    points = []

    for z in zips:
        for i in range(int(z[3]/minvalue)):

            points.append([[z[1],z[2]],z[0]])

    npset=np.array([row[0] for row in points])
    sample_size=int(min_samples/minvalue)
    estimator=DBSCAN(eps=eps,min_samples=sample_size,n_jobs=-1)

    estimator.fit(npset)
    cluster_centers=[]
    for i in range(terrs):
        cluster_centers.append([0,0])

    groups={}
    for i,lbl in enumerate(estimator.labels_):
        if lbl>-1:
            g=groups.get(lbl,[0,0,0])
            g[0]+=points[i][0][0]
            g[1]+=points[i][0][1]
            g[2]+=1
            groups[lbl]=g
    clusters=len(groups)
    print(clusters,"clusters found. ",end="")

    for i,grp in enumerate(groups):
        g=groups[grp]
        cluster_centers[i][0]=g[0]/g[2]
        cluster_centers[i][1]=g[1]/g[2]

    crs.execute("select zipgroup from currentbases where cityid='None'")
    zipgroups = crs.fetchall()

    for i,z in enumerate(zipgroups):
        if i<=clusters:
            crs.execute("update newcentroids set centroid=st_point({},{}),x={},y={} where zipgroup={}".format(cluster_centers[i][0],cluster_centers[i][1],cluster_centers[i][0],cluster_centers[i][1],z[0]))

def initialize_centroids():
    crs.execute(\
        "drop table if exists newcentroids;"
        "create table newcentroids (zipgroup integer,centroid GEOGRAPHY(Point,4326), x double precision, y double precision);"
    )
    for zg in range(terrs):
        crs.execute("insert into newcentroids values ({},NULL,0.0,0.0)".format(zg))

def initialize_currentbases():
    sql=(\
        "delete from currentbases;")
    crs.execute(sql)
    for zg in range(terrs):
        sql="insert into currentbases values ('None',0.0,{},'None',{},'00000',0)".format(zg,target_goal)
        crs.execute(sql)

def initialize_hulls():
    crs.execute(\
        "drop table if exists hulls;"
        "CREATE TABLE hulls (zipgroup integer,hull GEOGRAPHY );"
        "create unique INDEX hulls_zipgroup on hulls using btree (zipgroup);"
        "CREATE INDEX hulls_hull on hulls using gist (hull);")
    for zg in range(terrs):
        crs.execute("insert into hulls (zipgroup, hull) values ({},NULL )".format(zg))

def find_base_cities(zipgroup=-1):
    if zipgroup==-1:
        sql=("select cb.zipgroup, cityid from currentbases cb, newcentroids nc "
                    "where targets=0 and cb.zipgroup=nc.zipgroup and nc.x<>0")
    else:
        sql=("select cb.zipgroup, cityid from currentbases cb, newcentroids nc "
            "where cb.zipgroup={} and cb.zipgroup=nc.zipgroup and nc.x<>0".format(zipgroup))
    crs.execute(sql)
    zipgroups=crs.fetchall()

    crs.execute("select zipgroup, cityid from currentbases where targets>0")
    usedbases=crs.fetchall()
    newbases=[]
    for base in usedbases:
        newbases.append(base[1])

    for zg in zipgroups:
        if zipgroup==-1:
            sql=(\
                "select bc.cityid,name, state ,bc.zip,cb.zipgroup "
                "from basecities bc,newcentroids nc, currentbases cb "
                "where cb.zipgroup=nc.zipgroup and cb.zipgroup={} and nc.x<>0"
                "order by bc.citypoint::geometry<->nc.centroid::geometry".format(zg[0]))
        else:
            sql=(\
                "select bc.cityid,name, state ,bc.zip,cb.zipgroup "
                "from basecities bc,newcentroids nc, currentbases cb "
                "where cb.zipgroup=nc.zipgroup and cb.zipgroup={zg} and nc.x<>0 and bc.zip in "
                "(select zip from assignedzips where zipgroup={zg}) "
                "order by bc.citypoint::geometry<->nc.centroid::geometry".format(zg=zg[0]))

        crs.execute(sql)
        newcities=crs.fetchall()

        for nc in newcities:
            if zg[1] == nc[0]:
                break
            else:
                if nc[0] not in newbases and nc[0] not in tried_bases:
                    newbases.append(nc[0])
                    tried_bases.append(nc[0])
                    sql="update currentbases set cityid='{}',zip='{}' where zipgroup={}".format(nc[0],nc[3],zg[0])
                    crs.execute(sql)
                    crs.execute("commit")
                    break
    crs.execute('commit;')


def assignzips(drivetime=1,goal=0):
    sql=(\
        "select cb.zipgroup, cityid, targets, goal,zip "
        "from currentbases cb, newcentroids nc "
        "where cb.zipgroup=nc.zipgroup and nc.x<>0 and nc.y<>0")
    crs.execute(sql)
    allbases=crs.fetchall()
    allbasetotals={}
    allbasetotals[-1]=9999999

    for b in allbases:
        allbasetotals[b[0]]=b[2]

    i=1
    for base in allbases:

        togroup = base[0]
        totaltargets=group_total(togroup)
        to_goal=goal
        if totaltargets<to_goal:
            center_zip=base[4]
            moved_zips=[]

            if center_zip > '00000' and center_zip < '99999':

                if G.node[center_zip]['zipgroup']!=togroup:
                    totaltargets+=G.node[center_zip]['targets']
                    allbasetotals[togroup]+=G.node[center_zip]['targets']
                    change_zip_group(center_zip,togroup)
                    moved_zips.append([center_zip,-1,togroup,totaltargets])
            print(".",end="",flush=True)

            sql=(\
                "select az.zip,az.targets,az.zipgroup,cz.drivetime, cz.cityid, cz.miles "
                "from "
                "assignedzips az, citytozip cz "
                "where az.zip=cz.zip  and  cz.cityid='{}' and cz.drivetime<{} and az.targets>0 "
                "order by cz.drivetime".format(base[1],drivetime)
                )

            crs.execute(sql)
            zips=crs.fetchall()

            moved=0
            assigned=0
            zt=0
            assign_the_zip=True
            for z in zips:
                if totaltargets>=to_goal:
                    break
                zip=z[0]
                targets=z[1]

                fromgroup=z[2]
                if fromgroup==-1:
                    zt+=targets
                if fromgroup>-1:
                    from_goal=9999999
                else:
                    from_goal=goal
                if fromgroup==-1 \
                        and (totaltargets+targets<=to_goal*(1+over) or totaltargets<to_goal) \
                        and (fromgroup==-1 or allbasetotals[fromgroup]-targets>=from_goal*(1-under)):

                    assign_the_zip=True
                    path=[]
                    try:
                        path=nx.shortest_path(G,zip,center_zip,weight='miles')
                    except:
                        reason="no path found from {} to {}".format(zip,center_zip)
                        assign_the_zip=False
                    if assign_the_zip:
                        pathtotal=totaltargets
                        for n in path:
                            if G.node[n]['zipgroup'] not in [-1,togroup] and n!=zip:
                                assign_the_zip=False
                                reason="zip {} on path in group {}".format(n,G.node[n]['zipgroup'])
                            else:
                                t=0
                                if G.node[n]['zipgroup']!=togroup:
                                    t = G.node[n]['targets']
                                if pathtotal+t>to_goal*(1+over):
                                    assign_the_zip=False
                                    reason="too many targets on path {} to {} - zip {} fromgroup {} togroup {}".format(pathtotal, pathtotal+G.node[n]['targets'],zip,fromgroup,togroup)
                                else:
                                    pathtotal+=t

                    if assign_the_zip:
                        for n in path:
                            fg=G.node[n]['zipgroup']
                            if fg>-1 and fg!=togroup:
                                if assign_the_zip:
                                    assign_the_zip=still_connected(n,fromgroup)
                                else:
                                    reason="moveing zip {} disconnects group {}".format(n,fromgroup)
                                    break

                    if assign_the_zip:
                        assigned+=1
                        allbasetotals[fromgroup]-=targets
                        moved+=1
                        for n in path:
                            #if n=='75243':print("path:",n,G.node[n]['zipgroup'],G.node[n]['targets'])
                            pf = G.node[n]['zipgroup']
                            if pf!=togroup:

                                totaltargets+=G.node[n]['targets']
                                allbasetotals[togroup]+=G.node[n]['targets']
                                #if zip=='75243':print("change",n,togroup,totaltargets,allbasetotals[togroup])
                                change_zip_group(n,togroup)
                                moved_zips.append([n,fromgroup,togroup,totaltargets])
                        totaltargets=group_total(togroup)
            to_total=group_total(togroup)
            if to_total<to_goal*(1-under):
                for z in moved_zips:
                    to_total-=G.node[z[0]]['targets']
                    #if z[0]=='75243':print(zip,togroup,z,to_total,to_goal*(1-under),moved_zips)
                    change_zip_group(z[0],z[1])
            allbasetotals[togroup]=group_total(togroup)

def group_total(zipgroup):
    crs.execute("commit;"
                "select sum(targets) from assignedzips where zipgroup={} group by zipgroup;".format(zipgroup))
    total=crs.fetchall()
    if len(total)>0:
        return total[0][0]
    else:
        return 0

def still_connected(zip,zipgroup):
    #returns true if removing zip from zipgroup keeps zipgroup in a connected grouping
    good_move=True
    if zipgroup>-1 and G.node[zip]['zipgroup']==zipgroup:
        fromnodes=(n for n in G if G.node[n]['zipgroup']==zipgroup)
        fromgraph=nx.subgraph(G,fromnodes)

        if fromgraph.number_of_nodes()>1:
            numparts = 1
            if not nx.is_connected(fromgraph):
                parts=nx.connected_component_subgraphs(fromgraph)
                numparts = len(list(parts))
            try:
                fromgraph.remove_node(zip)
            except:
                pass
            if not nx.is_connected(fromgraph):
                parts=nx.connected_component_subgraphs(fromgraph)
                plist=list(parts)
                if numparts < len(plist):
                    for p in plist:
                        if nx.number_of_nodes(p)>2:
                            if nx.number_of_nodes(fromgraph)-nx.number_of_nodes(p)>2:
                                good_move=False
                                break
    return good_move


def update_targets(goal=0,terr_type='None',update_only=False):
    sql= (\
          "update currentbases set targets = az.sumtargets "
          "from (select zipgroup, sum(targets) as sumtargets from assignedzips az group by zipgroup) az "
          "where currentbases.zipgroup = az.zipgroup;"
          "commit;"
    )
    crs.execute(sql)
    if update_only:
        return
    crs.execute("select zipgroup, sumtargets, sumtargets/area as density from "
                "(select zipgroup, sum(targets) as sumtargets, sum(st_area(geog)) as area "
                "from assignedzips az,zipshapes zs where az.zip=zs.zip and zipgroup>-1 "
                "group by zipgroup) a order by density desc")
    zgtotals=crs.fetchall()
    fulltime=0
    for zgt in zgtotals:
        if zgt[1]>=goal*(1-under):
            if terr_type!='None':
                sql="update currentbases set goal={}, terr_type='{}' where zipgroup={} and terr_type='None'".format(goal,terr_type,zgt[0])
                crs.execute(sql)
    sql="update currentbases set cityid='None', goal={} where terr_type='None'".format(goal)
    crs.execute(sql)
    crs.execute("commit")

def unassign_low_groups():
    crs.execute("select zipgroup from currentbases where cityid='None'")
    lowgroups=crs.fetchall()
    bases_needed = len(lowgroups)
    for zg in lowgroups:
        crs.execute("update assignedzips set zipgroup=-1 where zipgroup={}".format(zg[0]))
    assign_graph()
    return bases_needed

def fill_in_zips(zipgroup=-1,fromgroup=-1):

    updatesql=(\
        "update assignedzips set  zipgroup=b.zipgroup from ( "
        "select zl.zip, zipgroup from ziplocations zl, hulls h "
        "where h.zipgroup={} and st_intersects(zl.zippoint::GEOMETRY, h.hull)) b "
        "where b.zip=assignedzips.zip and assignedzips.zipgroup={}"
        )
    groups=[]
    if zipgroup==-1:
        crs.execute("select zipgroup from currentbases where targets>0")
        used_groups=crs.fetchall()
        for g in used_groups:
            groups.append(g[0])
    else:
        groups.append(zipgroup)

    for zg in groups:
        update_hull(zg)
        crs.execute("commit")
        crs.execute(updatesql.format(zg,fromgroup))
        crs.execute("commit")
        update_hull(zg)

def fill_in_holes(zipgroup):
        sql=("update assignedzips set zipgroup={zg} from"
             "(select tozip from "
             "(select tozip, geog::geometry as zippoly, st_makepolygon(geom) as poly from "
             "(select (st_dump(st_boundary(st_union(geog::geometry)))).* from "
             "assignedzips az, zipshapes zs where az.zip=zs.zip and az.zipgroup={zg} group by zipgroup ) a,"
             "(select tozip,geog from zipshapes zs, assignedzips az, ziptozips zz "
             "where zs.zip=zz.tozip and zz.fromzip=az.zip and az.zipgroup={zg} and zz.miles<50 ) b) c, assignedzips az "
             "where c.tozip=az.zip and az.zipgroup=-1 and st_contains(poly,zippoly) "
             "group by tozip) a where assignedzips.zip=a.tozip".format(zg=zipgroup))
        crs.execute(sql)

def update_hull(zipgroup):
    hullsql=(\
            "update hulls set hull=h.hull from("
            "select az.zipgroup, st_convexhull(st_collect(geog::geometry))::GEOGRAPHY as hull "
            "   from zipshapes zs, assignedzips az"
            "   where az.zip=zs.zip and az.zipgroup={}"
            " group by az.zipgroup) h "
            "where h.zipgroup=hulls.zipgroup")
    crs.execute(hullsql.format(zipgroup))
    crs.execute("commit;")

def fix_embedded_terrs():
    might_be_embedded_sql=(\
        "select h2.zipgroup,h1.zipgroup from hulls h1, hulls h2 where st_containsproperly(h1.hull::GEOMETRY, h2.hull::GEOMETRY )")
    really_embeded=(\
                   "select inside.zipgroup, outside.zipgroup, st_containsproperly(outside.geog::geometry, inside.geog::geometry) "
                   ", st_length(st_intersection(outside.geog::geometry,inside.geog::geometry)::geography ), st_perimeter(inside.geog)" \
                   "from " \
                   "(select distinct on (zipgroup) " \
                   "zipgroup, st_area(st_makepolygon(geom)::geography) as area,st_makepolygon(geom)::geography as geog " \
                   "from (select az.zipgroup,(st_dump(st_boundary(st_union(geog::geometry)))).* " \
                   "from assignedzips az, zipshapes zs where az.zip=zs.zip and az.zipgroup={inside} group by zipgroup) a order by zipgroup, area desc) inside, " \
                   "(select distinct on (zipgroup) " \
                   "zipgroup, st_area(st_makepolygon(geom)::geography) as area,st_makepolygon(geom)::geography as geog " \
                   "from (select az.zipgroup,(st_dump(st_boundary(st_union(geog::geometry)))).* " \
                   "from assignedzips az, zipshapes zs where az.zip=zs.zip and az.zipgroup={outside} group by zipgroup) a order by zipgroup, area desc) outside")

    sql=("update assignedzips set zipgroup={} where zipgroup={};commit;")
    crs.execute(might_be_embedded_sql)
    bases=crs.fetchall()

    for base in bases:
        inside=base[0]
        outside=base[1]
        embedsql=really_embeded.format(inside=inside,outside=outside)

        crs.execute(embedsql)
        result=crs.fetchall()

        embeded=True
        if len(result)==0:
            embeded=False
        if embeded:
            if result[0][4]>0:
                almost_embeded=result[0][3]/result[0][4]
            else:
                almost_embeded=0
            if result[0][2] or almost_embeded>.65:
                if result[0][2]:
                    print("{} is embeded in {}".format(inside,outside))
                else:
                    print("{} is {}% embeded in {}".format(inside,100*almost_embeded,outside))
                crs.execute(sql.format(outside,inside))
                sql2="update currentbases set cityid='None',terr_type='None',targets=0 where zipgroup={}".format(inside)
                crs.execute(sql2)
                crs.execute("commit")
            elif almost_embeded>.1:
                pass
            #print("{} is not embeded enough in {} at {}%".format(inside,outside,100*almost_embeded))


def make_graph(table='ziptozips'):
    G=nx.Graph()
    crs.execute("select fromzip, tozip, miles, drivetime from {}"
                "  where tozip>fromzip "
                "  and (connection='Small' or connection='Normal' or connection='Bridge')"
                "  order by fromzip".format(table))
    zips=crs.fetchall()
    fz='00000'
    for zft in zips:
        if zft[0]!=fz:
            fz=zft[0]
            G.add_node(zft[0], zipgroup=-1,targets=0)
        G.add_node(zft[1], zipgroup=-1,targets=0)
        G.add_edge(fz,zft[1], miles=zft[2],drivetime=zft[3])
    return G

def assign_graph():
    crs.execute('select zip,zipgroup,targets from assignedzips')
    zips=crs.fetchall()
    for z in zips:
        try:
            G.node[z[0]]['zipgroup']=z[1]
            G.node[z[0]]['targets']=z[2]

        except KeyError:
            G.add_node(z[0],zipgroup=z[1],targets=z[2])


def fix_islands():
    crs.execute("select zipgroup,zip,cityid from currentbases where targets>0")
    groups=crs.fetchall()
    debug='428400205384525 vvv'
    for g in groups:
        zg=g[0]
        base_zip=g[1]
        fromnodes=(n for n in G if G.node[n]['zipgroup']==zg)
        fromgraph=nx.subgraph(G,fromnodes)
        if fromgraph.number_of_nodes()>1:
            if not nx.is_connected(fromgraph):
                parts=list(nx.connected_component_subgraphs(fromgraph))
                if g[2]==debug:print("disconnected parts:",len(parts))
                for part in parts:
                    part_targets=0
                    zip_targets=0
                    for n in part.nodes():
                        part_targets+=part.node[n]['targets']
                        if part.node[n]['targets']>zip_targets:
                            zip_targets=part.node[n]['targets']
                            zip_with_targets=n
                    if g[2]==debug:print("group total less part total:",group_total(zg)-part_targets)
                    if group_total(zg)-part_targets>target_goal*(1-under) or part_targets==0 or part_targets<.2*group_total(zg):
                        for n in part.nodes():
                            nbrs=nx.neighbors(G,n)
                            connections={}
                            for nbr in nbrs:
                                if G.node[nbr]['zipgroup']!=zg:
                                    connections[G.node[nbr]['zipgroup']]=connections.get(G.node[nbr]['zipgroup'],0)+1
                        most_connections=0
                        most_connections_group=-1
                        for c in connections:
                            if connections[c]>most_connections:
                                most_connections=connections[c]
                                most_connections_group=c
                        if g[2]==debug:print("part most connected to:",most_connections_group," with this many connections",most_connections)
                        for n in part.nodes():
                            change_zip_group(n,most_connections_group)
                    else:
                        good_path=True
                        try:
                            path=nx.shortest_path(G,zip_with_targets,base_zip,'drivetime')
                        except:
                            good_path=False
                        if good_path:
                            for n in path:
                                if G.node[n]['zipgroup']!=zg and G.node[n]['targets']>0:
                                    good_path=False
                        if good_path:
                            for n in path:
                                if G.node[n]['zipgroup']!=zg:
                                    change_zip_group(n,zg)
    fromnodes=(n for n in G if G.node[n]['zipgroup']==-1)
    fromgraph=nx.subgraph(G,fromnodes)
    if not nx.is_connected(fromgraph):
        parts=nx.connected_component_subgraphs(fromgraph)
        for part in parts:
            if part.number_of_nodes()<10:
                for n in part.nodes():
                    next_to={}
                    nbrs=nx.neighbors(G,n)
                    for nbr in nbrs:
                        next_to[G.node[nbr]['zipgroup']]=next_to.get(G.node[nbr]['zipgroup'],0)+1
                    togroup=-1
                    touches=0
                    for zg in next_to:
                        if next_to[zg]>touches:
                            touches=next_to[zg]
                            togroup=zg
                    if togroup>-1:
                        change_zip_group(n,togroup)

def assign_nearby_zips(zipgroup=-1,fill_only=False):
    maxgoal=current_goal
    drivetime_factor=1.25
    if fill_only:
        maxgoal=target_goal*(1+over)
        drivetime_factor=1
    if zipgroup==-1:
        crs.execute("select zipgroup, sum(targets) as tottargs from assignedzips group by zipgroup order by tottargs ")
    else:
        crs.execute("select zipgroup, sum(targets) as tottargs from assignedzips  where zipgroup={} "
                    "group by zipgroup order by tottargs ".format(zipgroup))
    groups=crs.fetchall()
    for g in groups:
        total_targets=g[1]
        to_group=g[0]
        if total_targets<maxgoal:
            moved_zips=[]
            crs.execute("select az.zip, az.targets, cb.zip "
                        " from assignedzips az, currentbases cb, citytozip cz "
                        " where cb.zipgroup={} and cb.cityid=cz.cityid and az.zip=cz.zip "
                        " and az.targets>0 and az.zipgroup=-1 and cz.drivetime<{}"
                        .format(to_group,drivetime_factor*drivetime))
            zips=crs.fetchall()
            for z in zips:
                zip=z[0]
                targets=z[1]
                center_zip=z[2]
                assign_the_zip=True
                if total_targets+targets<=maxgoal:
                    try:
                        p=nx.shortest_path(G,zip,center_zip,weight='drivetime')
                    except:
                        assign_the_zip=False
                    if assign_the_zip and len(p)>1:
                        path_total=total_targets
                        hits=0
                        for n in p:
                            if G.node[n]['zipgroup']==to_group:
                                break
                            if G.node[n]['zipgroup'] not in [-1,to_group]:
                                hits+=1
                                if hits>2:
                                    assign_the_zip=False
                            if hits<=2:
                                if G.node[n]['zipgroup']==-1:
                                    path_total+=G.node[n]['targets']
                                    if path_total>maxgoal:
                                        assign_the_zip=False
                    if assign_the_zip:
                        for n in p:
                            if G.node[n]['zipgroup']==-1:
                                change_zip_group(n,to_group)
                                moved_zips.append(n)
                                total_targets+=G.node[n]['targets']
                else:
                    break
            #print(to_group,total_targets,( len(moved_zips)>0 and total_targets>target_goal*(1+over) and total_targets<2*target_goal*(1-under)))
            if len(moved_zips)>0 and total_targets>target_goal*(1+over) and total_targets<maxgoal \
                    and not fill_only:
                #print("reversing",to_group)
                for z in moved_zips:
                    change_zip_group(z,-1)
                    total_targets-=G.node[z]['targets']

def swap_zips():
    print("trading zips")
    crs.execute("select zipgroup, targets, goal from currentbases where targets>0 order by targets desc")
    zipgroups=crs.fetchall()
    grouptargets={}
    groupgoals={}
    for g in zipgroups:
        grouptargets[g[0]]=group_total(g[0])
        groupgoals[g[0]]=g[2]

    for g in zipgroups:

        zg=g[0]
        targets=grouptargets[zg]
        #print("{}:{} - ".format(zg,targets),end="",flush=True)
        if targets>target_goal*(1+over):
            border=find_border_zips(zg)
            touches=[]
            for n in border:
                nbrs=nx.all_neighbors(G,n)
                for nbr in nbrs:
                    ng=G.node[nbr]['zipgroup']
                    nt=G.node[n]['targets']
                    if ng>-1 and ng!=zg \
                            and grouptargets[ng]<targets :
                        touches.append([n,ng,nt])
                        break
            for n in touches:
                z=n[0]
                ng=n[1]
                nt=n[2]
                if targets-nt>grouptargets[ng]+nt:
                    if still_connected(z,zg):
                        change_zip_group(z,ng)
                        targets-=nt
                        grouptargets[zg]-=nt
                        grouptargets[ng]+=nt
            crs.execute("commit")

def find_border_zips(zg,unassigned_only=False):
    border_zips=[]
    basenodes=(n for n in G if G.node[n]['zipgroup']==zg)
    for n in list(basenodes):
        nbrs=nx.all_neighbors(G,n)
        for nb in nbrs:
            if unassigned_only:
                if G.node[nb]['zipgroup']==-1:
                    border_zips.append(n)
                    break
            else:
                if G.node[nb]['zipgroup']!=zg:
                    border_zips.append(n)
                    break
    return border_zips


def trim_big_groups(zipgroup=-1):
    if zipgroup==-1:
        crs.execute("select zipgroup, targets, goal, cityid from currentbases where targets>goal")
    else:
        crs.execute("select zipgroup, targets, goal, cityid from currentbases where zipgroup={}".format(zipgroup))
    biggroups=crs.fetchall()
    for g in biggroups:
        zg=g[0]
        total=g[1]
        group_goal=g[2]
        boarder_zips=find_border_zips(zg,unassigned_only=True)
        zip_values={}
        for z in boarder_zips:
            zip_values[z]=G.node[z]['targets']
        zips_sorted=sorted(zip_values.items(),key=operator.itemgetter(1))
        # if g[3]=='488400208104111':
        #     print(zipgroup, zips_sorted)
        for zp in zips_sorted:
            z=zp[0]
            if total-G.node[z]['targets']>group_goal:
                if still_connected(z,zg):
                    change_zip_group(z,-1)
                    total-=G.node[z]['targets']
    update_targets(update_only=True)


def trim_edges():
    sqltrimzeros=(\
        "update assignedzips set zipgroup=-1 from "
        "(select az.zipgroup, az.zip, az.targets from assignedzips az, ziplocations zl, "
        "(select zipgroup, st_concavehull(st_collect(zippoint::geometry),.99)::geography as hull "
         "from assignedzips az, ziplocations zl "
         "where az.zip=zl.zip and az.zipgroup>-1 and az.targets>0 group by zipgroup) h "
        "where az.targets=0 and zl.zip=az.zip and az.zipgroup=h.zipgroup "
        "and st_disjoint(hull::geometry,zippoint::geometry)) a "
        "where a.zip=assignedzips.zip;"
        "commit;"
        )
    print("Cleaning up boarders - Removing edge zips with no targets")
    crs.execute(sqltrimzeros)

def change_zip_group(zip,group=-1):
    crs.execute("update assignedzips set zipgroup={} where zip='{}'".format(group,zip))
    G.node[zip]['zipgroup']=group
    crs.execute("commit")
    #if zip=='75243':print(zip,"moved to group",group)

def find_centroids(zipgroup,weighted=False):
    weight='1'
    if weighted:
        weight='targets'
    sql = (\
               "update newcentroids set x=nc.x, y=nc.y, centroid=st_point(nc.x,nc.y) from "
               "  (select a.zipgroup, x/ziptotal as x, y/ziptotal as y from "
               "     (select az.zipgroup, sum(st_x(zippoint)*{tgts}) as x, sum(st_y(zippoint)*{tgts}) as y, sum({tgts}) as ziptotal "
               "     from assignedzips az, ziplocations zl "
               "     where az.zipgroup={zg} and az.zip=zl.zip and az.targets>0 group by az.zipgroup)  "
               "     a) nc "
               "  where newcentroids.zipgroup=nc.zipgroup".format(tgts=weight,zg=zipgroup)
               )
    crs.execute(sql)
    crs.execute("commit")

def color_terrs():
    print("Coloring terrs")
    Z=nx.Graph()
    sql=(\
        "drop table if exists hulls_final;"
        "create table hulls_final as (select zipgroup, st_convexhull(st_collect(geog::geometry)) as hull "
        "from assignedzips az,zipshapes zs where az.zipgroup>-1 and zs.zip=az.zip group by az.zipgroup);"

        "select h1.zipgroup as zg1, h2.zipgroup as zg2 "
        "from hulls_final h1, hulls_final h2 "
        "where st_intersects(h1.hull, h2.hull) and h1.zipgroup<>h2.zipgroup")
    crs.execute(sql)
    zipgroups=crs.fetchall()
    for zg in zipgroups:
        Z.add_edge(zg[0],zg[1])
    sql=(\
        "select zga,zgb,miles from "
        "(select zga, zgb,count(*) as misses,max(miles) as miles "
        "from (select cb.zipgroup, hull "
        "from hulls_final h,currentbases cb, basecities bc where bc.cityid=cb.cityid and cb.zipgroup=h.zipgroup) h,"
        "(select a.zipgroup as zga, b.zipgroup as zgb,a.citypoint as cpa, b.citypoint as cpb, "
        "st_distance_sphere(a.citypoint,b.citypoint)*0.000621371 as miles from "
        "(select cb.cityid, cb.zipgroup, citypoint "
        "from basecities bc, currentbases cb where cb.cityid=bc.cityid) a, "
        "(select cb.cityid, cb.zipgroup, citypoint "
        "from basecities bc, currentbases cb where cb.cityid=bc.cityid) b "
        "order by a.citypoint <-> b.citypoint) p "
        "where not st_intersects(h.hull,st_makeline(p.cpa,p.cpb)) "
        "and h.zipgroup<>p.zga and h.zipgroup<>p.zgb and p.zgb>p.zga "
        "group by zga,zgb) a where (miles<300 and a.misses>={}) "
        "or (miles<600 and a.misses>={})".format(totalterrs-5,totalterrs-2))
    crs.execute(sql)
    zipgroups=crs.fetchall()
    for zg in zipgroups:
        Z.add_edge(zg[0],zg[1],miles=zg[2])

    colors=color_graph.greedy_color(Z, strategy=color_graph.strategy_smallest_last)
    for zg in colors:
        crs.execute("update currentbases set color={} where zipgroup={};commit;".format(colors[zg],zg))


def split_group(zipgroup,terr_number_offset=1000):
    update_hull(zipgroup)
    update_targets(update_only=True)
    goal=int(group_total(zipgroup)/2)
    if goal>target_goal*(1+over):
        goal=target_goal
    newgroup=terr_number_offset+zipgroup
    crs.execute("insert into hulls values ({},null)".format(newgroup))
    crs.execute("insert into newcentroids values ({},NULL,0.0,0.0)".format(newgroup))
    crs.execute("insert into currentbases values ('None',0.0,{},'None',{},'00000',0)".format(newgroup,target_goal))
    crs.execute("commit;")
    sql=("select length/points "
         "from (select st_perimeter(hull) as length,st_npoints(hull::GEOMETRY ) as points "
         "from hulls where zipgroup={}) a".format(zipgroup))
    crs.execute(sql)
    dist=crs.fetchall()
    average_distance=dist[0][0]

    sql=("select p1,p2,st_astext(pnt1) as pt1,st_astext(pnt2) as pt2,sum(targets) "
         "from assignedzips az, ziplocations zl,"
         "(select p1,p2,pnt1, pnt2,geom from "
         "(select c.path as p1, c.point as pnt1, b.path as p2,b.point as pnt2, (st_dump(st_split(hull::geometry,st_makeline(b.point,c.point)))).* "
         "from hulls,"
         "(select path[2] as path,st_x(geom) as x,st_y(geom) as y, geom as point from "
         "(select (st_dumppoints(st_segmentize(hull,{ad})::geometry)).* from hulls where zipgroup={zg}) a) b,"
         "(select path[2] as path,st_x(geom) as x,st_y(geom) as y, geom as point from "
         "(select (st_dumppoints(st_segmentize(hull,{ad})::geometry)).* from hulls where zipgroup={zg}) a) c "
         "where hulls.zipgroup={zg} and b.path>c.path ) d "
         "where path[1]=1) e "
         "where az.zipgroup={zg} and zl.zip=az.zip and st_intersects(e.geom,zl.zippoint) "
         "group by p1,p2,pt1,pt2".format(ad=average_distance,zg=zipgroup))

    crs.execute(sql)
    cuts=crs.fetchall()
    p1=p2=-1
    pnt1=pnt2=None
    goal_diff=999999
    for cut in cuts:
        targs=cut[4]
        if abs(targs-goal)<goal_diff:
            goal_diff=abs(targs-goal)
            p1=cut[0]
            p2=cut[1]
            pnt1=cut[2]
            pnt2=cut[3]
    if pnt1 is None or pnt2 is None:
        print("**Problem splitting group",zipgroup)
        crs.execute('delete from hulls where zipgroup={}'.format(newgroup))
        crs.execute('delete from currentbases where zipgroup={}'.format(newgroup))
        crs.execute('delete from newcentroids where zipgroup={}'.format(newgroup))
        return
    sql=("update assignedzips set zipgroup={} from ziplocations zl, "
         "(select geom from ("
         "select (st_dump(st_split(hull::geometry,st_makeline(st_geogfromtext('{}')::geometry,st_geogfromtext('{}')::geometry)::geometry))).* from hulls "
         "where hulls.zipgroup={zg}"
         ") a, currentbases cb, basecities bc "
         "where bc.cityid=cb.cityid and cb.zipgroup={zg} and not st_intersects(citypoint,geom)) b "
         "where assignedzips.zipgroup={zg} and zl.zip=assignedzips.zip and st_intersects(zl.zippoint,b.geom)".format(newgroup,pnt1,pnt2,zg=zipgroup))
    crs.execute(sql)
    update_hull(zipgroup)
    update_hull(newgroup)
    find_centroids(zipgroup)
    find_centroids(newgroup)
    find_base_cities(zipgroup)
    find_base_cities(newgroup)
    adjust_for_changed_centroid(zipgroup)
    adjust_for_changed_centroid(newgroup)
    update_targets(update_only=True)
    return newgroup

def adjust_for_changed_centroid(zipgroup):
    sql=("update assignedzips set zipgroup=-1 from "
         "(select cz.zip from citytozip cz, currentbases cb , assignedzips az "
         " where az.zipgroup={zg} and cb.zipgroup={zg} and cb.cityid=cz.cityid and az.zip=cz.zip and cz.drivetime>{dt}) a"
         " where assignedzips.zip=a.zip".format(zg=zipgroup,dt=drivetime))
    crs.execute(sql)
    crs.execute("commit;")

##############################################################################################################
cnx = psycopg2.connect(user='postgres', password='Fred0fred', database='optimizer', port='5432')
crs = cnx.cursor()


##################################
# Number of Territories
goal_terrs = 90



terrs = goal_terrs*20

# Targets per Territory for Full Time
target_goal = 154
# Drivetime set in hours

drivetime = 2









starting_drivetime = drivetime

# min_sample_percent >0 and <1 use a larger number if lots of zips have data

min_sample_percent = .2

# Percent range allowed around target goals

over=.05
under=.05

# fill_nearby true to add extra zips at edges
# set to false if lots of zips have data

split=False
fill_nearby=False

# Allow 3 Day Flex Reps
flex_3 = False

drivetime_multiplyer_3=1
target_percent_3=.6

# Allow 2 Day Flex Reps
flex_2 = False

drivetime_multiplye_2r=1
target_percent_2=.4

# Allow 2 and a half Day Flex Reps
flex_2p5 = False
drivetime_multiplyer_2p5=1
target_percent_2p5=.5

# Allow 1 Day Flex Reps
flex_1 = False
drivetime_multiplyer_1=1
target_percent_1=.2


# Starting Miles between targets

#################################

max_eps=drivetime*60/69

maxloop=50


goals=[]
goals.append(['full time',int(round(target_goal,0))])
mingoal=int(round(target_goal,0))
if flex_3:
    goals.append(['flex 3 day',int(round(target_percent_3*target_goal,0))])
    mingoal=int(round(target_percent_3*target_goal,0))
if flex_2p5:
    goals.append(['flex 2.5 day',int(round(target_percent_2p5*target_goal,0))])
    mingoal=int(round(target_percent_2p5*target_goal,0))
if flex_2:
    goals.append(['flex 2 day',int(round(target_percent_2*target_goal,0))])
    mingoal=int(round(target_percent_2*target_goal,0))
if flex_1:
    goals.append(['flex 1 day',int(round(target_percent_1*target_goal,0))])
    mingoal=int(round(target_percent_1*target_goal,0))


G=make_graph('ziptozips')
assign_graph()

initialize_assignedzips()
initialize_currentbases()
initialize_centroids()
initialize_hulls()

min_samples=999999
passnumber=0
print(goals)
for goal in goals:
    tried_bases=[]
    avg=0
    i=0
    if min_samples>int(goal[1]*min_sample_percent):
        min_samples=int(goal[1]*min_sample_percent)
    last_bases_needed=99999
    print("\n"
          "Creating {} territories with a goal of {} targets within a {} hour drive".format(goal[0],goal[1],drivetime))
    initial_target_miles_apart=10*drivetime/2
    eps_initial=initial_target_miles_apart/69
    eps=eps_initial

    update_targets(goal=goal[1],terr_type=goal[0])
    find_startingpoints()
    find_base_cities()
    assign_graph()
    if passnumber==0:
        bases_needed=terrs
    retries=0
    while i<maxloop and last_bases_needed>=bases_needed and bases_needed>0:
        passnumber+=1
        i+=1
        last_bases_needed=bases_needed

        print_needed = goal_terrs-(terrs-bases_needed)
        print("pass {}:  {} territories still needed"
              .format(passnumber,print_needed))
        assign_graph()
        update_targets(goal=goal[1],terr_type=goal[0],update_only=True)
        assignzips(drivetime,goal[1])
        print(flush=True)
        #input("Paused...")
        update_targets(goal=goal[1],terr_type=goal[0])
        bases_needed=unassign_low_groups()
        if bases_needed==last_bases_needed:
            if retries>1:
                print("territories needed has not changed")
                break
            else:
                retries+=1
                min_samples*=.95
                min_samples=max(min_samples,5)
                eps+=.05
        else:
            retries=0

        assign_graph()
        eps+=.0125
        eps=min(eps,max_eps)
        min_samples*=.99
        min_samples=max(min_samples,5)

        find_startingpoints()
        find_base_cities()
        assign_graph()
    min_samples*=.8
    min_samples=max(min_samples,5)
    eps+=.1
    # drivetime_multiplyer+=1
    # drivetime=starting_drivetime*drivetime_multiplyer

crs.execute("drop table if exists assignedzips_temp;"
            "create table assignedzips_temp as (select * from assignedzips);"
            "commit;")

print("Filling in territories")
fill_in_zips()
print("patching holes")
update_targets(update_only=True)
assign_graph()
fix_islands()
update_targets(update_only=True)
assign_graph()
print("Moving Centroids")
crs.execute("select zipgroup from currentbases where targets>0")
adjust_groups=crs.fetchall()
tried_bases=[]
for zg in adjust_groups:
    print(".",end="",flush=True)
    find_centroids(zg[0],weighted=True)

    find_base_cities(zg[0])

    adjust_for_changed_centroid(zg[0])
print(flush=True)


i=0
current_goal=2*(1-under)*target_goal
offset=0
while i<2 and split:
    i+=1
    if fill_nearby:
        print("filling up territories with nearby targets")
        assign_graph()
        update_targets(update_only=True)
        assign_nearby_zips(fill_only=False)
    assign_graph()
    update_targets(update_only=True)
    print("filling territory with stray zips")
    fill_in_zips()
    assign_graph()
    update_targets(update_only=True)

    print("combining embedded territories")
    fix_embedded_terrs()
    update_targets(update_only=True)
    assign_graph()


    some_split=True

    while some_split:
        offset+=10000
        assign_graph()
        update_targets(update_only=True)

        crs.execute("select zipgroup, targets from currentbases where targets>{}".format(current_goal))
        bases=crs.fetchall()

        some_split=False
        if len(bases)>0:
            print("splitting terrs with more than {} targets".format(current_goal))
        for base in bases:

            if base[1]>current_goal:
                print("Splitting group",base[0])
                newgroup=split_group(base[0],terr_number_offset=offset)
                newtotal=group_total(newgroup)
                if newtotal>0:
                    some_split=True
                    adjust_for_changed_centroid(base[0])
                    adjust_for_changed_centroid(newgroup)
                    update_targets(update_only=True)
                    assign_graph()
                    if fill_nearby:
                        assign_nearby_zips(zipgroup=base[0],fill_only=True)
                        assign_nearby_zips(zipgroup=newgroup,fill_only=True)
                        update_targets(update_only=True)
                        assign_graph()
                    fill_in_zips(base[0])
                    fill_in_zips(newgroup)
                    update_targets(update_only=True)
                    assign_graph()
        fix_islands()
        update_targets(update_only=True)
        assign_graph()
        fix_embedded_terrs()
        update_targets(update_only=True)
        assign_graph()
        current_goal*=.99
        if current_goal<2*(1-under)*target_goal:
            some_split=False


fix_islands()
update_targets(update_only=True)
crs.execute("select zipgroup from currentbases where targets>0")
zipgroups=crs.fetchall()
for zg in zipgroups:
    adjust_for_changed_centroid(zg[0])
update_targets(update_only=True)
assign_graph()
fix_islands()
update_targets(update_only=True)

crs.execute("select terr_type, count(*) from currentbases where cityid<>'None' group by terr_type")
goodterrs=crs.fetchall()
totalterrs=0
for gt in goodterrs:
    totalterrs+=gt[1]
color_terrs()
sql=(\
    "update assignedzips set drivetime=a.drivetime from "
    "(select az.zip, cz.drivetime "
    "from assignedzips az, currentbases cb, citytozip cz "
    "where az.zipgroup=cb.zipgroup and cb.cityid=cz.cityid and cz.zip=az.zip) a "
    "where assignedzips.zip=a.zip")
print("territories created: {} with a max drive time of {}\n".format(totalterrs,drivetime))

crs.close()