package com.querydsl.jpa.test;

import com.querydsl.core.Tuple;
import com.querydsl.jpa.entity.Member;
import com.querydsl.jpa.entity.QMember;
import com.querydsl.jpa.entity.QTeam;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @PersistenceContext
    private EntityManager em;

    private JPAQueryFactory queryFactory;
    private QMember member = QMember.member;
    private QTeam team = QTeam.team;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
    }

    @Test
    public void startQuerydsl() {
        QMember m = new QMember("m");

        Member findMember = queryFactory
                .select(m)
                .from(m)
                .where(m.username.eq("member1"))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchOptions() {
        // JPQL이 제공하는 모든 검색 조건 제공
        queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1") // username = 'member1'
                                .and(member.username.ne("member1")) // username != 'member1'
                                .and(member.username.eq("member1").not()) // username != 'member1'
                                .and(member.username.isNotNull()) // 이름이 is not null

                                .and(member.age.in(10, 20)) // age int 10, 20
                                .and(member.age.notIn(10, 20)) // age not in 10, 20
                                .and(member.age.between(10, 30)) // between 10, 30

                                .and(member.age.goe(30)) // age >= 30
                                .and(member.age.gt(30)) // age > 30
                                .and(member.age.loe(30)) // age <= 30
                                .and(member.age.lt(30)) // age < 30

                                .and(member.username.like("member%")) // like 검색
                                .and(member.username.contains("member")) // like %member% 검색
                                .and(member.username.startsWith("member"))
                ).fetch();
            /*
            * fetch() : 리스트 조회, 데이터 없으면 빈 리스트 반환
            * fetchOne() : 단 건 조회,
            * - 결과 없으면 : null
            * - 결과가 둘 이상이면 : com.querydsl.core.NonUniqueResultException
            * fetchFirst() : limit(1).fetchOne()
            * fetchResults() : 페이징 정보 포함, total count 쿼리 추가 실행
            * fetchCount() : count 쿼리로 변경해서 count 수
            * */
    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순 (desc)
     * 2. 회원 이름 오름차순 (asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     * */

    @Test
    public void sort() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();
    }

    @Test
    public void  paging1() {
        List<Member> members = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 0부터 시작 (zero index)
                .limit(2)
                .fetch();
    }

    /**
     * 집합 함수
     * JPQL
     * select
     *  count(m),   // 회원수
     *  sum(m.age), // 나이합
     *  avg(m.age), // 평균나이
     *  Max(m.age), // 최대 나이
     *  min(m.age)  // 최소 나이
     *
     * */
    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);

        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라
     * */
    @Test
    public void group() {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();
        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamB.get(team.name)).isEqualTo("teamB");
    }

    /**
     * 팀 A에 소속된 모든 회원
     * */
    @Test
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }


    /**
     * 세타 조인 (연관관계가 없는 필드로 조인)
     * 회원의 이름이 팀 이름과 같은 회원 조회
     * */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL : select m, t from m left join m.team on t.name = 'teamA'
     * SQL : select m.*, t.* from member m left join team t on m.team_id = t.id and t.name = 'teamA'
     * */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }
}
