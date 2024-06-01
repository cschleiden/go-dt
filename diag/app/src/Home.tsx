import { Pagination, Table } from "react-bootstrap";
import { Link, useLocation } from "react-router-dom";

import React, { useState } from "react";
import useFetch from "react-fetch-hook";
import { LinkContainer } from "react-router-bootstrap";
import { WorkflowInstance, WorkflowInstanceState } from "./Components";
import { WorkflowInstanceRef } from "./client";

function useQuery() {
  const { search } = useLocation();

  return React.useMemo(() => new URLSearchParams(search), [search]);
}

function Home() {
  const count = 20;

  const query = useQuery();
  const afterId = query.get("after");
  const page = +(query.get("page") || 1);

  const { isLoading, data } = useFetch<WorkflowInstanceRef[]>(
    document.location.pathname +
    `api/?count=${count}` +
    (afterId ? `&after=${afterId}` : "")
  );

  const [instanceIdFilterValue, setInstanceIdFilterValue] = useState("")

  return (
    <div className="App">
      <header className="App-header">
        <h2>Instances</h2>
      </header>

      {isLoading && <div>Loading...</div>}

      {!isLoading && (
        <>
          <Table striped bordered hover size="sm">
            <thead>
              <tr>
                <th>
                  Instance ID<br />
                  <input type="text" defaultValue=""
                    value={instanceIdFilterValue}
                    onChange={
                      (e) => {
                        setInstanceIdFilterValue(e.target.value)
                      }
                    }
                  ></input>
                </th>
                <th>Parent Instance ID</th>
                <th>Created At</th>
                <th>Completed At</th>
                <th style={{ textAlign: "center" }}>State</th>
              </tr>
            </thead>
            <tbody>
              {(data || []).
                filter(e => (
                  e.instance.instance_id.
                    includes(instanceIdFilterValue)
                )).
                map((i) => (
                  <tr key={i.instance.instance_id}>
                    <td>
                      <Link
                        to={`/${i.instance.instance_id}/${i.instance.execution_id}`}
                      >
                        <WorkflowInstance instance={i.instance} />
                      </Link>
                    </td>
                    <td>
                      {i.instance.parent && (
                        <Link
                          to={`/${i.instance.parent.instance_id}/${i.instance.parent.execution_id}`}
                        >
                          <WorkflowInstance instance={i.instance.parent} />
                        </Link>
                      )}
                    </td>
                    <td>
                      <code>{i.created_at}</code>
                    </td>
                    <td>
                      <code>{i.completed_at}</code>
                    </td>
                    <td style={{ textAlign: "center" }}>
                      <WorkflowInstanceState state={i.state} />
                    </td>
                  </tr>
                ))}
            </tbody>
          </Table>

          <div className="d-flex justify-content-center">
            <Pagination>
              <LinkContainer to="/?">
                <Pagination.First disabled={!afterId} />
              </LinkContainer>
              <Pagination.Item active>{page}</Pagination.Item>
              <LinkContainer
                to={`/?after=${(data &&
                  `${data[data.length - 1].instance.instance_id}:${data[data.length - 1].instance.execution_id
                  }`) ||
                  ""
                  }&page=${page + 1}`}
              >
                <Pagination.Next disabled={!data || data.length < count} />
              </LinkContainer>
            </Pagination>
          </div>
        </>
      )}
    </div>
  );
}

export default Home;
