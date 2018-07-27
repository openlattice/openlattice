/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.data

import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.entityKeyIdsClause
import com.openlattice.data.storage.selectEntitySetWithPropertyTypes
import com.openlattice.data.storage.selectVersionOfPropertyTypeInEntitySet
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEntityDataQueryServiceTest {
    private val logger: Logger = LoggerFactory.getLogger(PostgresEntityDataQueryServiceTest::class.java)


    @Test
    fun testEntitySetQuery() {
        val entitySetId = UUID.fromString("ed5716db-830b-41b7-9905-24fa82761ace")

        val propertyTypeMap = mapOf(
                Pair(UUID.fromString("c270d705-3616-4abc-b16e-f891e264b784"), "im.PersonNickName"),
                Pair(UUID.fromString("7b038634-a0b4-4ce1-a04f-85d1775937aa"), "nc.PersonSurName"),
                Pair(UUID.fromString("8293b7f3-d89d-44f5-bec2-6397a4c5af8b"), "nc.PersonHairColorText"),
                Pair(UUID.fromString("5260cfbd-bfa4-40c1-ade5-cd83cc9f99b2"), "nc.SubjectIdentification"),
                Pair(UUID.fromString("e9a0b4dc-5298-47c1-8837-20af172379a5"), "nc.PersonGivenName"),
                Pair(UUID.fromString("d0935a7e-efd3-4903-b673-0869ef527dea"), "nc.PersonMiddleName"),
                Pair(UUID.fromString("45aa6695-a7e7-46b6-96bd-782e6aa9ac13"), "publicsafety.mugshot"),
                Pair(UUID.fromString("1e6ff0f0-0545-4368-b878-677823459e57"), "nc.PersonBirthDate"),
                Pair(UUID.fromString("ac37e344-62da-4b50-b608-0618a923a92d"), "nc.PersonEyeColorText"),
                Pair(UUID.fromString("481f59e4-e146-4963-a837-4f4e514df8b7"), "nc.SSN"),
                Pair(UUID.fromString("d9a90e01-9670-46e8-b142-6d0c4871f633"), "j.SentenceRegisterSexOffenderIndicator"),
                Pair(UUID.fromString("d3f3f3de-dc1b-40da-9076-683ddbfeb4d8"), "nc.PersonSuffix"),
                Pair(UUID.fromString("f0a6a588-aee7-49a2-8f8e-e5209731da30"), "nc.PersonHeightMeasure"),
                Pair(UUID.fromString("fa00bfdb-98ec-487a-b62f-f6614e4c921b"), "criminaljustice.persontype"),
                Pair(UUID.fromString("5ea6e8d5-93bb-47cf-b054-9faaeb05fb27"), "person.stateidstate"),
                Pair(UUID.fromString("6ec154f8-a4a1-4df2-8c57-d98cbac1478e"), "nc.PersonSex"),
                Pair(UUID.fromString("cf4598e7-5bbe-49f7-8935-4b1a692f6111"), "nc.PersonBirthPlace"),
                Pair(UUID.fromString("32eba813-7d20-4be1-bc1a-717f99917a5e"), "housing.notes"),
                Pair(UUID.fromString("c7d2c503-651d-483f-8c17-72358bcfc5cc"), "justice.xref"),
                Pair(UUID.fromString("f950d05a-f4f2-451b-8c6d-56e78bba8b42"), "nc.PersonRace"),
                Pair(UUID.fromString("314d2bfd-e50e-4965-b2eb-422742fa265c"), "housing.updatedat"),
                Pair(UUID.fromString("1407ac70-ea63-4879-aca4-6722034f0cda"), "nc.PersonEthnicity")
        );
        val entityKeyIds = sequenceOf(
                "ce2288a9-bfe4-40c9-b565-d15fd9b34ea4",
                "0cdad600-41ce-4af4-bd7b-4f752a173c3b",
                "45ce04c9-c865-49c2-89ff-883614b6b168",
                "ec746ecf-197c-4436-a4eb-65658381f828",
                "6341e054-4c86-4e58-8e9e-282619e37f00"
        )
                .map(UUID::fromString)
                .toSet()
        val version = System.currentTimeMillis()
        logger.info(
                "Entity set query: {}",
                selectEntitySetWithPropertyTypes(
                        entitySetId,
                        Optional.of(entityKeyIds),
                        propertyTypeMap,
                        setOf(MetadataOption.LAST_WRITE, MetadataOption.LAST_INDEX),
                        propertyTypeMap.keys.map { it to (it==UUID.fromString("45aa6695-a7e7-46b6-96bd-782e6aa9ac13")) }.toMap()
                )
        )
//        logger.info("Versioned query: {}", selectEntitySetWithPropertyTypes(entitySetId, propertyTypeMap, setOf(MetadataOption.LAST_WRITE, MetadataOption.LAST_INDEX), version))
    }

    @Test
    fun testPropertyTypeQuery() {
        val entitySetId = UUID.fromString("2fc1aefc-ceb8-4834-a9fd-b203d382394c")
        val entityKeyId = UUID.randomUUID()
        val propertyTypeId = UUID.fromString("00e11d1a-5bd7-42bd-89a5-a452d4f6337e")
        val fqn = "publicsafety.assignedofficer"
        val version = 1525826581101L
        logger.info(
                "SQL Query: {}",
                selectVersionOfPropertyTypeInEntitySet(
                        entitySetId, entityKeyIdsClause(setOf(entityKeyId)), propertyTypeId, fqn, version,false
                )
        )
    }

    @Test
fun testBase64Decode() {

        val base64 =
                "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAIBAQEBAQIBAQECAgICAgQDAgICAgUEBAMEBgUGBgYF\n" +
                "BgYGBwkIBgcJBwYGCAsICQoKCgoKBggLDAsKDAkKCgr/2wBDAQICAgICAgUDAwUKBwYHCgoKCgoK\n" +
                "CgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgr/wgARCADcASsDASIA\n" +
                "AhEBAxEB/8QAHQAAAAcBAQEAAAAAAAAAAAAAAQIDBAUGBwgACf/EABkBAAMBAQEAAAAAAAAAAAAA\n" +
                "AAABAgMEBf/aAAwDAQACEAMQAAAB5WckXkOr5SQy4LyvLCrTBUTCARMMB8KQj7yZhL5oTpKoOdM4\n" +
                "HUTUDxwMPxhCkKhVICAsViRVgEzI6Ins3X3IvXWGnyPcoueqDrEWGZyi5UqLJqgJyKJ+8oDA8YBB\n" +
                "4KcFupWcxutW+PgfW7RdMmMHR7rnfVMpvCqC+aMbxxgYTi8CoJJEX820BcqNl615P6w59Pkusg46\n" +
                "oVWQXYs5brCXOmpLUOmZpUU/DVRLlQBTn8y962e7P8tqCbSRz0zkNVXRj6OtxWmcbsGHSG3Ltiqa\n" +
                "kZmMCiXh8emQpxhoJukWtk6u5Q6vx0+TC6S/VCqxHEnlinoOduQHZY9qOTTiQBln7td6O3KTnj7Z\n" +
                "x0k65ugHikutE5Fx5RW03oBC5RtWa9PNo1xxLbPQ81cUxxSpymo8YhoBKYgbJ1Tyv1Rjp8m3CK3T\n" +
                "CzhsLHKTduh02j2FVJN48g3wx5pKjYoGxrd2/rKOW+orZfa+Xe4vIpPn6dEJj0n1cltia5Py08+0\n" +
                "GmOaj0ZzR0t6nluFUz5QqPlJC+MMsPGCzYep+WupebT5PmQN1QoVFrTXj0mVNRNMZZwIYZvAIRco\n" +
                "lL5dLeepRMN75AqgrtashIcXVn05Q5Hr5dGrkNY8xSu36sRWQ9K88bz6nlTByGMFVElJpQSnEUT+\n" +
                "T17qPl7qHDX5KAmj1wZl5rQVP3poTEOgB96gR8YbWdh7Vx95VZpxwdtfYW+Hp2N/Ie5OmsQl9X0x\n" +
                "ijW5zKo8DZKtvnM1yDuHTnblCqd3hnUIeUJynoMAeT2HqDl/qDDX5CJlQ7YBIyaYAApiYpgEQOHj\n" +
                "mOxhaKZZuPuvM1TrD53o0SMlYbswtT6vr07Td2L7h6LTCpRGZH0uyUHq5l7clYtsZE5D9vjqCQzp\n" +
                "QxTI94pmbD1Dy/1BzafHYhC9yEgEliZIyFTJnTUUSOxUUzChJyHd49VjjTqYdAOfWmrYvLHG49ET\n" +
                "ZoV8c8vELwXPq3rs9FdnJpEs2X6vNVEpyDCBkCYp2CYPD2DqHl3qLn1+NpCl7kJQJDMKRgUVbKsc\n" +
                "KN7IiFHTXuN44x3bCtLWeVh0quTWZn+XsqFrl1IprAtq/tz3NKnOE5Z1U73rjp8X1hVdOHn8+sZn\n" +
                "KZnTG0uZJRNQyZ5Nf6h5e6hw2+MZU/diMQCDP4goOpKdBy6e+bTWOlCs2ptkDylunPVVDEmGnVEl\n" +
                "ZaA4h6GwpgjlWbdaoKorLJubB5ty77h0b80vq92cHIfQ0xj+JXKb2Jya5jlq3Ng/Mh4Wy9ScsdT8\n" +
                "2/xZBJXuRZCb3JPGtl3KW5agapq/J9lj6A86l4Dp1bIq56yW8Z70E+jIK47QHphO4iRmHLmNfzEp\n" +
                "Fwsk/dY6sIeXYjz/AOi/z87B7OPepeJmVxZdomFdJ57fNey3nP8AR2lWnSEx0P1Vyd1jz7fE6403\n" +
                "qHtm2ac2m+Onlfg8w2pK6G2O1GQ1nosoYe4SWV8eYb3hgNVnTsjyes7hJ1lab1N7k3D5gEW8jFFr\n" +
                "ljKyFrRjfRc1Id3Bq19r7i+DEdaaUKdLTxx9E+EyqQRYu66Q64437I4dPjX3jyV3RtDbPJHENKf6\n" +
                "I4CCT0jO7q04yfYsJ0W5mWtWNZzKTBay45zj6i5Vz93Bw2iR5fSoC1xbyq49scrnUFM2R1lcZ2Va\n" +
                "3vq+Rgcc6vG3Pc8ov2drGZ5V6w5O1fZGEv7yVw+1XY6HRHZ3FvaXFp//xAAvEAAABgIBBAEEAgEE\n" +
                "AwAAAAAAAQIDBAUGERIHEBMhMRQiNkEIFTIWICQmIzRC/9oACAEBAAEFAmy9JBFsaBECL2RaCSBF\n" +
                "sySNH2+DLsQ9D2PZAu5Ah72RD12IFoaBjRA/lRAyHQEtZiEEEkEkCLYSn2RDQSRgi3213PuQ3oj+\n" +
                "UmCBD0QSYIgQJPciGgfo+OxrY4jgOgZazEILRILQItAiCSBAiBDQIaGgRaLXvs642wmyzWJFcfzO\n" +
                "0dQrJbkwzl100KrOYkhyPKYko+AXcglI+ASQohoGka9mQ6EeswCS9JIF7BAgkfoh+iIu+hsGZEV7\n" +
                "lUeCmdbzrExxMhxMGk0jQgWUusdo8vhzwk+QT8kNAuxEDIcTIlewotGY6FFrLwgJBewRBIL4Iuxa\n" +
                "BD4LXZ51DKL7LHpayQtZ/SrUEwnlGuufQaK19aTq5RkUNxJnFURcNHj2XPwVRX2ZSARbMkjQ4jRg\n" +
                "iIGkKIgeiHQwtZcEfBAgQIvaQRgvYL0CMiHMiHMG8SU5XeJsHI9dsmo6CJpjYajEPHokRS2iK2sv\n" +
                "6wuK61gw5j8VwWtEuIMSvF1s4tKBex+tAvjuoKIx0KSosuCPgiBENGYIgWwXocga9F5QcgiPzjJL\n" +
                "JUeBAY8iuBBokkbSE6QjiokGGmnDVEhKcVLjG0lZff4hOipkMzWFR38MtDnVZK2CBAgfyY/+v1ri\n" +
                "Ohxf9rCC9ERaT8pIgWtEfszCnQt8KeMgbo8gyJ/zT69OoxF6Z3zYWZpSf3NJIeNtIhfaqc4slOmR\n" +
                "jkgOcdXTZKcwiYTFwgx70W+xaPv8Ax0P/LAgglJhPsFohsKcC3iC3yILfIG8PMPIJ5+ayaQTMdhn\n" +
                "RMso5x4rZt+HiGmtmhlYrKviLOBwUqPonWuIT7TfEkn6x5yNZNcjT7MJHz/t1s+iH5YEj4BbHMGs\n" +
                "OOkQdkGFv+zdMczMthKz3JQSbQ1IbS7MmyVG5dtBiZcGdROd2zKQk7Oc8yTWcZPXG71IvJyWsltU\n" +
                "uM2UOxBpUSr5rT7ivujkrw/4mj41o9DiCIa0Ne+iPrKwk9Dfrlocy0t3Qde0Fuchvt+gQlsbslsp\n" +
                "Utp1DQa8TrS6olsJUphyvaXKOwjKaLil1Va3GNT5scZVZGWqP5iRkZaZWjgmD/63raQXofogXsv2\n" +
                "OifrKgRjkFKC3CDjphSgXYu5BfFb5tbbmuvRHGshtkCLlmQHGdkJlpx0jZEs2HU2lm9EnVOeSYTk\n" +
                "nLYc92IxIcC4fiatmCNhmK5Ik109qSRaCNbLun2Xbop+VDfo1BbgecClbHo+/oH2INc/rSbW6SKp\n" +
                "l4LxyGEwmmkkylDtUoiRwQ89a4+wa2MYiEuHWRmUqiqIOm5xsCLx4nBXZQm6/wDqsuSWgQIuxEEj\n" +
                "126KflI3olrILcCjMwfz8j99y7NnuVFJIQkvGiIp4SFMQRErJMh2ropSm59TZw3H7NEmaqrJ9VdW\n" +
                "cF2UeOaJejE9XAqu7t6pKFLtZZAtguxAhvY2W+imv9Ugz2FGFGD9AxsEfYuxAhHWSZcY9CsJKw7w\n" +
                "jtTpkqPJbu7VoRMulMk1k2YIjyn5F0qllmbBO7TZKPlMURItniUGTdsabH4hwy9dyHwPjt/iOin5\n" +
                "UN+lmQMHrtrYLt7BfJAv8WzNE5lekw5amjlWCCYsJ0dKo8OymrXCyB42XbWG5R2UJRORzivuStFK\n" +
                "lksTJCCRKd8wppC41bXxyYaBex+vQ/fbQ6KflI5bBmDMctjYL2CGgRhPz+yMKIylxHPtbe0HJUt1\n" +
                "UZhts1XcDm1fQPBGuosgEjkhq3eUJL6jORIPctxxaXuXGFCNhJAy9eiLehsyH7IfrQ6J/lQ3637G\n" +
                "xsgRgj7ENjex8FJ0mY2rQQoJWS0NQq+QqBjdOtMfFqQSMcx5Boq4sV1KWlLlGe5BmFrSZtoddsi+\n" +
                "UfJey9Ahvt63vt0UTrKxsbB+xvZmNj4CT2C+d7BGY37sIskmGn1IR9WoRZXFRsrMKas1GzCu9woc\n" +
                "rit9uITtgtYRY8kvPJJEmShIxmO9Z3T+N30IF8p+d7G9BI1sEoldvRjon+UjYMGNjfYgRjfuoxq0\n" +
                "uAxiNLCJl+piIgz3bqbfQjprRt0gy9pyukNrbSvgIMltTsmStlywmEofUaS7PUoG8rSnvIvGpSqK\n" +
                "bRWVfnNNLeqJcmZ09rrAWFbPqJIIEORdiHRT8qHIxsbH6372OYb5uuY7gbUFq8yhqG3W1OYZmUHG\n" +
                "8Wr5FFJkw49s6VhIdZNKiWZHX2fhJi9TtvImmCXkvMSLIlkuWSkokvGannZAqopNicpTUH+NedEz\n" +
                "I6tYk5LpKnPyq7BhVRl9VlWHWGMPbCfglbGxsyHRL8rHIbHIbMb7VGO3N4eHdO4+MtZflz7k2g6c\n" +
                "JQq5lRI1VjuJ1+O0Ob5IxEweKvyMOJ+5xkiPxGZkl8z8L2vp3UGmOZgiQlDcdxwm4giNpQVm5yjY\n" +
                "lZSqW7+sg5dil3h+K9TsbqcfuOmdqz9NbQOo2P3uDTI9026iNIQ+nexsdET3lo2NgkLWG4DixU4P\n" +
                "eXbtB0gqK4qHGkujqVat1sXp7i8ozpHoJ01jFbtMqycymH1RkQ1Xda7ttbfriRqWyRGlKx53EhPk\n" +
                "WbUFRkiMSUsQlKS2xwJSTSUtXNb3KBafx8v5VxgWANWzTXUTHjvcY6bZOm8gZHTRL6lYiyK1+O94\n" +
                "VNSW1lv30NPeWD1plpbyoNYt4Yn070K+pabQ1X/WSsoyOsxSDduWuUXH0ysaxeWhqhx7CatmfYZK\n" +
                "TNXcZ2wVVErDMls/Co6VmuMpBrj7BRiNLEZxJt16nCYittrJhxRmyhpD7hmIsY3XslYJuT/FCWha\n" +
                "WZS6rOrc7OCHPo8U6lxlE8x1gxcqHLGmjU15j4wbI3R0JVvLhvYo6x2UvBsPiwQ0kjDbam2bvLWM\n" +
                "ZZlS35T+FVCGrSweYyCZ1JZRWwOnceU5j+QV7kqblGPz7o4qVMPtp9JDbZGr6RpYKvWEQpARDUQb\n" +
                "aYbDr/EOKNYU1xTBhm2mbRrt7X+PkWUznmfuqrLGoktWD3WvDJERWI2DdhV9V8ZPIsRYe5ltLcnR\n" +
                "EvoGvnlohoU4/wBOMXT/AFdVEMo8RhRKzHLziOtEuQ5Dxs2300EapqcYipcyDqjY/wDJppjdfGbi\n" +
                "m8IlE3y6rdOZWPyYZeRom+IbSe+ZpHJIQskD6nSVP8gZLWG2FLCIBIFHjtjcy8lwqHj8TpFFkVeR\n" +
                "dQcdat6PD+U3HcmU9YYj0wt0LZTpacwpCxbLZ5GZtf8AkT/HpxZ5sMYrnbO1h0jUCMwyZLzfM2IZ\n" +
                "VVZNtpEODGrRWtuz8quZqGIODy2W4K1/6h6ht8WncTKVNxyTT+osCvuIOd9GLzBJCofkSiIvbkRa\n" +
                "kpjrSo0rIMsGsNxDM49S46Ga5DJUWOWF3Z4dgMHCqrMYrZuYLFiw7uz+1nGLH+tw5KztscpLyTVZ\n" +
                "BSWLNrX9fKUlsL/5DFYszR/Hb84HQ5lD2d15E4rqLdTqw0ME9Oq4zMeA36ndOHlnlh6mOMOqT0y6\n" +
                "QkU29jtoNzp4n/qVUr75sduLIpXVOsdSOlGGSaiq4y2TjNKSuDHMzhsmGIUclR4rPl8SNU0Fu1yL\n" +
                "GMYo8Yr7RxRFlilKks1MEskdfcfr8ckKcrKBJTrK9hsQco6S2kxNj1SjtSMIZM/KwXBz+Of5uP/E\n" +
                "ACYRAAICAQMEAgMBAQAAAAAAAAABAhEDECExBBITIEFRIjJCFGH/2gAIAQMBAT8BLL96+dFrsXWs\n" +
                "ONeH6Iq2VpRXrekODj1sSZHE2Lp7P8qH0yRLp2icHH14IcaWLWjFHcRErSzqIqhl6PSHGtemI8kY\n" +
                "mLMmyTVWPPDuIzhNmZfgTN9Pg+CHHvhjsLJGL3FNS4O38DyYk6YvHPgyRfYShKrFuXrHgXtgXcLF\n" +
                "FnjSdI7fxPDCYscIGSqGlLCP0jwL2wS2IzonN92zPNl/oU4/ZaM0vgzTUOn9GQ498TG+5GOMm9iW\n" +
                "Of2jtqQ3RlZKcp86fOsOBC9Oxn6HkIpSXJSguSWVtnlHJzP2Kob21h+vpVmy40Q4kcjiPLKRbEmR\n" +
                "VRMqadikp7HbufGkONVVlt6Wb9x22PGdgsYsX2P6R1ENi6P+6UQ/UbI8aWuC/wAiTpEaEJfYoxNh\n" +
                "2xR7d2Zpd8WyjHvHWHA2IfJF7kObJs7mzp+o/mR49jxixnaoqzqOplk2XBkdYxbmJ7tF1LSHB//E\n" +
                "ACgRAAICAQMEAgICAwAAAAAAAAABAhEDEiExEzJBUQQiEGEUI0Jxgf/aAAgBAgEBPwHajwcscfIh\n" +
                "uykVYqsfsrY/Z+hy+xT5F7K2L8GTvKL9ifkVSRKrFZsJNvYUF55NkOp8nTrdC0uRav8ADYrFuZO4\n" +
                "bYuDaJTPJTFFvYlkjBUiXyZC+Wz+V7RH5cJIWnLujwNISo3QtkZO4ZXos2Zq3GSeiBMlSewnS/Yo\n" +
                "+UVLwfDnJTMiqdjfkbkbCMncK2bpllP/AIL0h8mXhHS1cvcyYNK5NMpOj+PNx5JQnBez47XU2J9u\n" +
                "xyPgpmxk7hvYSiWqGLgXszTSkPDKW8RwnDk1/wBlIXx5abNM4cmOdZuByi6Qnf4dFk6ctj/Y9/xR\n" +
                "xwcqzLLSzqy98HVtWzUrI58kHySlO9yDuRCHTyWvJURo3Y9zJ3FV+HRbLPBnipSHjuVGOP13R046\n" +
                "fqlZKPiikj4+O3Zit5aoX7K8mpHgyXqLLjZvQlucFKtjJbIRcHaNWOtzXiW1ckqqiCT7jFWo0xXB\n" +
                "9jkS1D2ZN3I8FryWaRvbc68OKMf2+r5Om4jlKMqRKWSZHGjorVaNMYSVmqaW6NmcoViX23J9x4/F\n" +
                "MlkjFbEoPVqmPLS9fpEF/kJ2SxIhCEEzSiUkidynZki2lJElj4XJFuxp+T9GTuORyjFWTyZJx9Ix\n" +
                "4qe5JarYqlt6I10tizWa0nQ8rW5LK5ITbIy1YWS/skjLH0J3BSH3GTvG9KslercxwvcnwbUiN7tG\n" +
                "KbvRRw6G5eDV7HqIqMUa3keiBgm4fVlxeG/J9mtJjtfUS8oy9xkleSiOFR3Nd8GStkTX9bkiH0ia\n" +
                "N7ROUZOvJNzxuiOTWSzsTlnnpMeKONfUwpa3YlScTSlIyx6U1RxIzVr2P//EAE0QAAEDAgMFBAYE\n" +
                "CQoEBwAAAAEAAgMRIQQSMQUQE0FRImFxgRQgIzKRsgZCddEVM1JyobGzweEWMENTYmV0gpLwJTRV\n" +
                "wkRFVGSD0vH/2gAIAQEABj8CA9WwVldVV1putusvPdf1r/DdX+YrvxP2Y/8AaR79PWpuv6mm7RWH\n" +
                "q0rZVWquN9N2Wm64VPUxNf8Apjv2ke4epT+ZurbuJLIABqSuHhGcWnMGyyxlsY7gsv4Qf5KhxWb8\n" +
                "4IR7QZwTT3+X8FxMNM17erTVWVCqfFW9S3ILqrN3absR9mv/AGkfq1G+w9TxVVorlVRgwjg6Xn3K\n" +
                "uJmJHJu6i5+K1Wi42ElLe7kU2DGOEcxNhyPguzvoVUKlF96sqV32WI+zXfPH62it1Wu+iqv3KiMk\n" +
                "jqCidh8EcsZ+tzKsuy2vkuyxx8lRzCqMH5qzcC7VkfER0VQFW6GF2g4vh5OPvNQfDKHNI5FZfj6l\n" +
                "d1FVfu3Yj7Od87N1zusq7tFVa7rhaLRXTcDhHuyMd2yDZxWaT4KjWr3VoqUCJpRUV2eVEWujHeqA\n" +
                "0uuM1wom4OZ/spXUv9VWPqV333arEk/9Of8AtGetUqqtvsVqtU5jX0c+wXEf1Vyu0tFSisu0ELc0\n" +
                "W9yy7nRlEP1zIRSSZpITlPWnLdbfYq2/RT/ZzvnZvpvqrbtVYqlee/h5rNFKLNTVd6tQKuXTdpdV\n" +
                "LvBdl36FehCzcNaBVI07042Xo5faVtLqtd2isPUC8liPs9/zx76q+7RWVaqlVY7tVcp4trSybHVd\n" +
                "O9VROYCnVWNfBW81Q+S4rr+ayBuqo4DvojdEHknWudFFKzVrxlQtei91eKuv3+pdYj7Pd87N2m7o\n" +
                "rct1F09XzVPynLPJYLLhWENWbI4L2srjbRUkVC5VgNzpVAw5HAfVfHVZMRgWh3MtiXFkw5LK3FKW\n" +
                "VIiY3/kuRouNS9OuqzVuOib+bffRaeriPs93zs3+a1Vd1Rut6sUmXVXZW3NaU8Fnyk+S48LqEaii\n" +
                "uhlGoWWRmiy2aqGdv+pZOw4clxsMcjxeoCHGXEHJy7Op0UdBTsCyv0XnuBCpuvuxA/u93zs3/r3U\n" +
                "r/Ms6iqpVdjD5l7DCs7qtX/LQPadYhGQVG8QFjiO008imvPJSekV0pZPjiwhcGfWPNVGxyfzX/wV\n" +
                "tl4iORxvQIPLSARzK9p06Is6hRxMgee1V1EI+G5p5B27TffdbdP9nu+dm/VUVN2m/XfqnV0y1CAa\n" +
                "sqzH9ayYVnn0WXU1WY8lw3tVafwWZpt4ppihZYWKzR8lQonu5qLBYKOETPjPFlc8A25CqDDXth2a\n" +
                "/RUorKlN2nPdYbp/8A752bqrXdX16bnU7t1YwvaEkLhBuZ7vdYvduV6O1ntO8oPbCbfkoQzQhjst\n" +
                "hTVZom060WXEMr1VI+XciK3Raoxh35YXtNHU17RWD2m27mvdxD/l9TVVpRee+f8AwDvnZu1Wq1/m\n" +
                "bBSAdf3bsrgiWjteCdivrZbdyGLnbG8E09m648kXeiSOd3BfhHD4aCOLNTLNOMx/ypmLjiyytuaB\n" +
                "BsuoV7WoFRxt3rMjy71Bs/Cwhz4cziemY8yhBW4FZD3q+77vVoFP9nu+dnq19S/qyVPvFWdqrISO\n" +
                "1y6LtipXEhwtG9XWTIYdmvFDc2oU07QwRaK2JGoVWMbWmlEZWGsbjbuWU8wgLaI5h5pxr8VDhaAN\n" +
                "4ILrIuP1uqpurTdRW5b5/s93zs31Wu6i7/V1Vk9UCsvZ36LjTOBd1doFwxiM58EY3Ysdk6FZcLNF\n" +
                "IOcNbFelbJf2w8eyrcLgvF2mhBCzVAHVUpWiND3FUDdaBNEk5kyCjajRXVO9Uru+9XG6td2I+z3f\n" +
                "Ozd5KxWu6w339XXUBA0N1d1LLK2lAvbOk83VC4jWYc1FtFmlEDa++cwRETG05uZZZ8NjJnGvumlE\n" +
                "Cah2qyh2t04jp9yLgdVBCP60E16A1VaclqtVpuvv03Tmv/l7vnZ6l/V+71Lr0kYSSjdXcM0VD5LK\n" +
                "Pig2qzstZdmQLMXn4L2pcfJZHUCvJ5Ix5qAaHuWYtHx5qxJdzumxwsJe+ZsUQ6mqriNkzjvbHm/U\n" +
                "u8C4VN+vqXWI/wAA752b6+rotVQIS4aHJF/XSWH8VXH4l07j35WqsGFijp0bqm7O2fhHTvldlytF\n" +
                "h1r0WJwMTs7IcQ9gf1oaLyQuuXeqOZo3mVwiPCyyZD4ItBDqkoF1dezdDhv+ta3JZ5JPJCGI9px+\n" +
                "Kwu1MuY4Wdk1O8Gq/Dex3CC1Y5p3UY882np/vwTtn/SDZMfFYaESNBI8HD9adL9HMdkd/UTuq0+e\n" +
                "o/SvRNpYR8Mg0a8aju67tVZa7tVOP7vd87N9FdVqtd4YxpJcaNAuSU3an0jbmfSrcMdG+PXwROYM\n" +
                "Y0WXpGGiGCwX/q8TYEdw5oYebHS7XxIdlyF2WPN+aP3rEbSdBFBhYWhgaBZoy5jQA97VPigPxsrn\n" +
                "jzNVVaIEPuCu2NBrVGQO98aq7jVdgG5rTostTRVazwWRxv0Co7WlypSPe4TqfBfyQx8p4WKbWFtd\n" +
                "H/x/cm7e+jsZGPwr2jIHV47HGmQ1OtSKLgbTZJhMT9aKdhbX4r0bHxNkaRVp5tPUdFneeNhnGkeI\n" +
                "A/Q7od1N2m6f7Pf87N1t9t//AA/Auc3+tdZvxX4X2o9smJp2DS0fh96GydiYUz4h1mgaIY36Ssdt\n" +
                "LadMwwrPxWH535I4WVjJSKty0q1vTxtRT7ckhd6fj2iGF2WvAEjg1ob0uQVHgsOcsm0JXzkVuI3P\n" +
                "JbXyp8FQo0aqDRdk+SoBU9yq4I1Iqql+q7Ar3oZRy+K9m34BVk8lI08xRQ4zDSEPw04kbTWxusV+\n" +
                "DsSXOnwLwCCRR2U0PxUcm2dn5+I2scrOzJFmAc2h/NLU36NbQ2gcVhndrZmLIo57BrG7+039I81w\n" +
                "Z4w9kjaPa7mvTMjJtmyy0inAvEeTX/fzVeHp0WZh8VXdiPs53zs9SjQgB2ieQWTBw5W85C2w80MV\n" +
                "tWuLlHJ/uDyQkdEGxM9xoCOFhPapRrBzTtsYfZ/Fne7/AJh/uRnx5+ATcRgCSw1pJl/GX97zTMDh\n" +
                "+zC6cCUDQDme6y2dsPZ2JFZ5XvpXRrInUP8ArLFLHs2JowuzohhocmlWqjuqrVZTbvWQo5H6cirx\n" +
                "iyDQ1Enl3r2TauRp2R3hZI+f1iqVr1XCCE/R116Bhi0TbPxQZm5Oab9Cpthy7QrwbFpLSPZySQfk\n" +
                "X7MUamiw44eKg9tgSb0lGlD36eaD3M4UgJbLFX8W8ahYjZePhEkM8JZK08x9/NOwcr/aQyOjc7rQ\n" +
                "0quIx1jqFVp792I+znfOzddZQhDAzXqhidox0HKPmfH7lkjYAAOSbg2N7Or16K3t4hzPZwN18T0U\n" +
                "GEimPGneXueG2aB/EpuGwjcow2FpG3qf4lDDZvxEAZ40CxeLxAcQxlLPINSVisdhfxuG2aMPhzI6\n" +
                "obLMS98h7msa0/8A6otlx56kVdnF6d/fVFq0XY1Q7Krw6daKuWviszY/IrKeyK3WZkQd+cNVxHu/\n" +
                "yhZnW7lSMLRaLaGEMQDzho5D4gkV+FFNhh2G4maVrbe9xYWTA/GDEL0vBUlw8r80+FBNWUoM4qad\n" +
                "/LS9dQ92yMTmwO0jUM0MMtLtcDdp7j3IOHMJ2Ohjyw7QbxB0Dx7w/UfiiDrlsg+M0cOiyS6rEj+7\n" +
                "n/tGb2wQx1c7RelYj2mXQ0Ro1Vp4BOwWy3CXHyfjJKVEH3lOnnlc57zme9xuSvT8VCWtghFOyS4k\n" +
                "nu/3dYbZULn5H4ppk7NOyyrufXKofQ4S55ffwovSy4RuxWJcaUrYHL+4rj6zyTmRgEdewO06x/sx\n" +
                "Qt/+RYnEYrM6Z35TQKEcqCwVHihBoQgd2Y8uStbxVgD+aqNiKq6g81mc6qytt4KlfBeKrlvRcMN9\n" +
                "nHeU/uUuHw8piil2bJpzc1zKD4ErZ30oeWsAa0TvebB0L6084ZMT+hGN7PZxuIzPHvONqfrUH0g2\n" +
                "A88TDe5C/UUNaNdrT+yagcsqhnbo9gNFP6PFmxGF9vAOpGo8xUIStuOSdHyJtbkvdWIP92u+ePc1\n" +
                "o6qXbuItU8ODw+sf3LtCgV281+CNmS+2/pJB9TuHehHEwue48lDCC2Sd7vJqllxLWzTOjoS4Vv4F\n" +
                "TywxsazDQUaGt/KNP1xv+Kdho7vbRrG9XHQLDbBZhpGmGNsIc7m7SvW9ytq7QiArhY+FDbQmgP7J\n" +
                "i4L21X8o8HEThJnDjUH4t/XwO65WqvuuiGlW8VmVGhB0l3cgo9l7Ng4mImNujRzc7uCj2Rge3lGa\n" +
                "eYi8j+ZUGIbHUenta53Rr45YyP8AUYz5FS412Z3oXtnQD+kjo5sje6rM48SFFhsTK01w4ie+MUBI\n" +
                "OSqjx2IbUgG4bzqQn4DPeN5AB+qrjULG7NEVIuJxIPzHXHwuPJNxTD7tkB8Vi2HT8GP/AGke6HZ+\n" +
                "HHakcGs8SVFsPDM9nh4mj4aoDLblQJ+xdj4hon/pZgfc7h3rJDy/GSO0amYLDscXOu+U6n+Cw+Gw\n" +
                "ktC3DSSyBo1oWAfrUs+JkHDw7C6WixeIkzVdiizkfxbWsdofy+J8Vh4dWRSOxMn+X3P0/qQfP7rO\n" +
                "0sTtD0Ls4uZ8hPOn+68kMXhwafWHRSbL2ph2SRSMySRvFnAqTaOzYJMZsg9oTNGZ+HHR46f2vjRZ\n" +
                "2GoPMKhaqBUIWTKunVC3NaUQo2p60TdnbLwpnxL705MH5TjyCc1juNi5r4nElvvf2R0aE6R/RYZr\n" +
                "pmcTEejyZC6/vYh3/YsQ0NDs0FHAovGD7Uc7Wtqekg7X6aeNVtPZslIXx4ufIHkjV7nDWncnTgir\n" +
                "ZiHZHWKj2hhXVa4LCfSCKO7CYZT3G4/TX4p0LSNFmJ01usU6uuy3/tI90Dnj3IXOHjT+Ke9wuodl\n" +
                "YF/DjlirI5vvG3VRYaR7i2Q3qVSFmUClAnSV92IU8ytszOuWMYxleQzyfcFIydoo/EwsPg6ZrT+h\n" +
                "enUHEfs+SZxI+u7M8n4lbcxeJ7b4RFGwuOjTm/8AqE4ZbMw5LR30WCv72GqfMlGOlq0oqwWuhxLr\n" +
                "GfSPC7POCxUTHSF2DIY2Q69ptKfvXFkYK15K7UewtP8Ad17iAyoNy81gdlTPcxmJlyvdHTMPCqGC\n" +
                "2NgWxNN3urVzz1cTcrhjSidmNU1jIi3g4CBzaON+xitfipeI7/woOvVbeiLRSEytZ/oD/mW0oMRU\n" +
                "gYh1KH/22EP/AHFYuCAGnF+s4lO2XxPYvYXZTyIWOErfcYHN8QUW96xWXkbLE/ZL/wBpFu//xAAm\n" +
                "EAACAgIBAwQDAQEAAAAAAAABEQAhMUFRYXGBEJGh8LHB0eHx/9oACAEBAAE/IUAHUE0TucaloSwu\n" +
                "FJJ2MuH7moHJA0GxUtHmVWtGxLWcuIAQiB8QA5A/ktUTy3s0ISRuMGq3CwJAdf5GD5oTml5gIsE1\n" +
                "zBTCBcJHPzCpi/0jEwZfY5vpGXOOZggkOM8Q+DiEYpbgcjrB0O81LPaBaRJagARizM65PQSL4qEN\n" +
                "7YqYuFUOA4HEyl0hgXQ5rMBrE5vuJgLOoBSKAEifBjRShHlagEm8sxpnZ/7EhoQIx/Z96TIWoTJX\n" +
                "VmAIyqwDMW9NRTT2Os4NwlEeRhF7lQbZ4wpoHEFFKgzndqYOTCgFPjBg0WAxqAROgoQYObcQ53mo\n" +
                "0BfogewzGv7MQhc3Lhbiyx8EQTWOrjQgAOIptiUK4ga+YT4vhQKgh1jjdxNmOLMbNUzXWIIkErmF\n" +
                "9jiEJYFoVoOCIKLT/neBlEhmkrte1sjvOpMFvAOXwwGEYLuiOgwU2gCupOIR+FCOyEcFxvdF4XWA\n" +
                "7AYlhunAlNHA/UuJ6uYACzCI3mCQ36QIBWRQMJwJs6alqGFsxRg5wxK1vgTPIxCu4AAz18ww4z1i\n" +
                "BvRYjZI1sKXY4cSgqN8w+IhUI5w0IxzvmCAJBuofeYnlkk6joP4MwFQEAfIQAcWGoGFvS6NiZsQI\n" +
                "PnfqACgelOpg1Loiu8Aii+IJoGDUAHwRY8uPmBKBg0jZPOtysyB7fcQSGAncOn0hCDxzCWfccTc/\n" +
                "EdnncXIDuoAPJDwvKBJ9I1HgcTLdCLgdP3A5s5EAYY/64DABidKEnoC/zahEQzsRr4GEUqzUJKpg\n" +
                "lUYfmG0whO0ZAXGMvVqaoinGPRTUChRQT/qIzsIFjiY6jhAoI1uUJ9q7zoff74gXoeIp0UUcffrh\n" +
                "Da4J0ZzAtUOGYJjfmZnx+mn8E5+czcHnEfIjahNMUAJy4X+IJMoRuneEOpjEDYHGEQIm7GOxIcmF\n" +
                "ZiB/ykLlo7oIqHCgw8GswEBiT2hdaJhiHoAjCYeRKRBiigzaclLmmG/cCNzyBzObmnImjDcdmFFt\n" +
                "yeNQMnbLP8hAiiBwIZ6iMLJwYCCMsSiUWoAIsGtqZXumwH0ER4BxAY10h7sBU4DDycjtK+xuciaC\n" +
                "yTQhIBm1/swBlP6wIX24uEmdg4ZEMQTy8wOfVgARyRgQ3Pe4OghNZLjEC6PONSpqRubbhnMiDbdw\n" +
                "hogH798Ti+jZbH0EKNsM2Yt7fiJc+e8DQayIVsjso4DrqYDMckw6fiMktI5OonWFwaGKPz6OJHmJ\n" +
                "PhAw7swo4RMDuxc2XaoEIA1ALpNsfeJm+TMOWd0bm6im3PKMKEIHBdmMp0KCiw8EQVBAE1HyDBmQ\n" +
                "RAWCc9HAo+EPcJTAhWISCINvY+qFw0cob/7AEUYHJ4hPKOROksEVA/qLDBNmSAx+4Cuk6gGQaBQi\n" +
                "h0aEyHk3+4mHr4mL94ADWUnACViAswAKOm7TkeL0jBVr/kzQiEEXLH36oqyMZUArguI0EFfJjxe9\n" +
                "9oEIUtDiHFA6RlOOrGoGvuCHMFDiQIdA1kSwNDtqD2sauEOoqk0gAWv7lIFpgsmVggPbQ+uCBAAD\n" +
                "JxgCBIKJMKc8diz0jqIDecdoA0kkYVHJqBVAs3zUy1DZ4LINYnAHWAQQhAAKnzM77RQC6tQDQN8u\n" +
                "CyyRpmN0At6gEM9PBCtIEWd5hAWCgzKhdTM9+ZRAa90JQ7hZZNTIHVwGDxxBMvDcFSBoJEYdW4lO\n" +
                "UuYSQx0amVvYyl/IEByzmGjoI1S+3Ak/Y+qi0jtL3uMchtK+/uLQt028oDuitw7zOA0vzBgZYoEB\n" +
                "5TPKMSIJDlqEsLQMDAsIQEIdmuggI4J9ZgCfJgngOe0KDMt3QFub/TqEfM5orda5hATaiRL89YWz\n" +
                "yg4WzpcZjGKHHvDgO/mM3UKsOsOICgexAi8IBW6+1KgiwtMS+BNU0LlZbpD1O8bXyjf1wM31jfeW\n" +
                "Mcx3BAEX0QbCQmAI+3BoDrzXMdBIIQJIz1ikbLj2Ig8vN0RC72WkIgIMVZzve1RKdsQEMPI7iEex\n" +
                "e8MO1SqIFlBVxO3jmL6I4QLiW9YSi/BiDUBuIku4Anh4hvJ+IK/FymbHSXY3Ey13ERXjpCXbQ++Y\n" +
                "ANJG4EBcbmb4qwf3CcuBAjdXj5jhjD+koWYJB1AzewDlqFCyk0XIieTOE/1B013AAd8KDXulZcw3\n" +
                "OXhFOACLidqKegbR7QKQIBIZX9jU0UPeAAe6YOAclc/iAFrlHQtD2qAUuQ1AtHWoOlej07hnmHFd\n" +
                "GdwIyvOJl+EIV2pYuFOURiUAHGQoohyIE7+BADy6QCGVQpw/+QpfUuGHK7n79MJdJHBCJrNWn0YF\n" +
                "9QJlFEFEDEBUcMVvxD5pBPslV7PoPmM8MQAQoPDAMhCBbR5lLAdTEYoHfAkT5E8C8wpAJGA5FP04\n" +
                "Szor79UFldHmEP8ACUbA1zmEIsry6zULPAxMqMYcNBFk8dIF6Xp9YviAUe6Gd7EMEB1gYVGbresw\n" +
                "GiG9QHUdggb9o2b94ANXihMWonpQEnw4GeujCPkbhQHgXMEBkoQzpwpQPq9wKHpYQuYxMPRo6hY2\n" +
                "E1GSPyJlQXAQwwo/kPfmLkvAj+v9gFpBSeT9/M1YQCH0nOGYUYjpSsQnkCOhtfczl+JRnJzGqCUs\n" +
                "QEfwMTYmAha3UublCDx+mEGLavE7IGV9QqG13dwnbPbmYZydxAyOkWOkJCziC6iRw6O5j5Ni8CkC\n" +
                "FBHe5e0Ak2qEboQrSZNmQf2zcpBjzIQQQA/7GUr2RIXZYhggpDg+qDhq2MS6IEWMZMHyjAOwe0Zl\n" +
                "3cD2H4BAzg5IQrsahviThDsqgAWHdwFFnxzAaQL/ACmeukAsBYzmBgK3qAoB+QIhQ8RQDx+mTyOp\n" +
                "aB5h+4nFxKxmqHMPJvnMBkAnwIA7CAAG/aC+6Bv2ZmAfCzGAcPNCCoHA0g1uvv8AYeEE7Ngw6DFo\n" +
                "AN2oMWZhb9nmWijchqzQ3KAgVrOcGVGLHLU3YiHYROI5DecVAhLaEbl6BD8IUjUx8r+IDANDmw2f\n" +
                "eUIktLzUCF/Z4jiUH51EaQLGHHa+0Goi+uYxhFCetXBy44gFhHo5IGR4M3jWTUMMxoFO74hABsG7\n" +
                "8ws3pOowAl7SinWpiRoYnR23G5HpB0SAQRqFIlAsb6CZBQyHNXQfWD30vOYDao1AxAImdp9I2WDM\n" +
                "J8PPa4cajImwCvb8xKcNND2z1+IG9cDi/vtElimfvWNRV7OL55junxZZMvnmDCQ2kIviYANZQH8E\n" +
                "gxLjAjGy47nU41xUORaXDyhMw9K/4JcjhhoEAA8XMr79p7oacpJJfmPzrcCNBXImGNQA7VHQg2G+\n" +
                "8BzAHkQgLdJxBhQcB1jmVNDDANlLMEgMTYpdGoGFAihDxzDDgoneVAMpGi5M3HwYiFdHzuLLzY0+\n" +
                "+jCGxIUFDGPvMXRSAF9IcGNQdYz/ALN25/sD3joVMBxmWThDuRHbjA1Y0mtRTgns8wsiWcEdokaH\n" +
                "eDIYNPlDlAzx1/TCK+wO4X5RqOQT79/EoBoOhCAB5WhHArHB1/YNh/364xRt4KGbJcmWImEFwUDO\n" +
                "D5I6DU7rFzM5Ik+8DUtrP3jVE6SUDIbCocADaR+YSn3sr5ggg0ACbCXQQGWbOIlZROZtK/3AjizP\n" +
                "JL8blYIIkDNH97wW9CWA8ZnIXYIAAHlCACj04A8uBDH9JmIjT4nRHWchR7Q2evmAqMMje5sN1GTb\n" +
                "vbgF3k5bp+iioAfjUOmOszJMQnl9u8f5LgSQYrUpRlApquUP3D97QEhc4HpvwqoID1jQ7X8woIiV\n" +
                "IMZMmL2EeATN8xJEAG/XpUKLJ2oUpSaMMwUCcA25g6AWAq6qHGRgGgAmRqkax/YTOgSIyaaPb8S4\n" +
                "LFstMAwWBgcA0CvOYYJoAXkvviHMwyjwOfj4iTaQZpD5ivcqwWUVt2ZzsGDvp30ZBAYMy2QapgD8\n" +
                "yDAEHQchBgOxisA2oQaNlCLJ0+ICaG8wPIT04lMr6Q/ROSBg2rMzAe6hF/C8Qkj4HEJFhhxORx1l\n" +
                "ImLgMkDgAZMDQtd5Ob2d4ZJeACvAjpBdvNZvGusHiZSiY3eZCgR/WQEBxQsCxAAABzhv7QcFVxuB\n" +
                "2CyoQ7BoBdFDQLrWKj0iYJMDsMJdKi7tV3fP1RrCPZAX5UCEgAIK8PxK9gG3Fxc8ygMAmYFfhCHd\n" +
                "THuqM4IMgg2iMUPYwxuECJxAYXcbllGa5chB794D/PAM5LrGIacfAB7mD8St4OCVzCJ5P3GM56xB\n" +
                "Z7ogSc3qFXx6XQdkPV7GEhQGANxyiuLHAnJLQ3EExKNDz/w4SFNqYu7cERbdFjT6w3qAOB0P3Evo\n" +
                "IcN5DRDV8liOyhTQjib1aNB0WoEcQuql7OF5SUwYdADjO6hqFhLEQOyzD0Agz23DYpcPVuLiCjyg\n" +
                "ALhmMpEh1/ydHJW3aNAYGWckBhMpMIIgbwvkZjdDtoinNZHnUN31XmYWNWD0qBJLACQUJTafWC0G\n" +
                "dfJ8DMxRmEgj07ohgFDMXZBzC66xQijnt4WvvNIuG3AKUw3OJPPvMZ9HFWC+8OtvtDdklODWrY0V\n" +
                "MiwAcIvYD4crDYmknT+3MZACgPEBRfZCw4Z3hcBTLWmLOqcwQcsOCBUWiBIeiJdUUAADswGkHt1U\n" +
                "NYFoezm/4eAAL5IaPURxdeodDoYVUrRA5hQMHgqBMggyHMOJMmP8gJUzxDcg4AhlC/qw8xcD71dr\n" +
                "8ReUkh1/aZWiXbM3ULJJimq5ON/DhU+oLJizYydHjrDemHAByVHqIoiGXAg2jKZFlGCIeTZrFL38\n" +
                "dFAxUayjI6DQ6IjKycJzn5L5hwiHwJf4iurFHiG4hciJC9HJsDWYmhrJOoaEE/RwN6iHgd/p9gUH\n" +
                "+kIX9g6bWrXEyILRD4HeMFFYaYAfSnqIRWEWAUzs/kifmUtkkP4gQBG6wwWgfeH3D7jMLt/sshDR\n" +
                "qtwB25NAh2H3hNNtQKJhnCUMc1cBWSDJAW80UsxMRJxigJgZYN21MQpZZ0RhC1gbgsUBveGTQaC3\n" +
                "5mQDVlRYoVY4hXSwRZC/Y9scGpdAcGq/cx2uQwMWU5Zx35IkLKUA3JKy6AbgX2z4hs5Y4GCHzeSD\n" +
                "KBsI7kCJNgNoPTXBG5YmPSeLwFGt9QDJ/wA/UwwSkyXyoBQsvtBThwZMKBlLPa/TrfEJbU19V97Q\n" +
                "bRyXWYA4BgTsW2elQbJiD50hJBFzOQvEHcZkv7sWPTEGCmsLfKeULD0SpVmzQJ7nMDyKnYIzEsgu\n" +
                "Az3LMUhpU02PYjoyGzhyM1MfapwB6rQQgxoapeC0aKw3+BD872NY6R1ZEE6HHcZgOySjGgj3ArE/\n" +
                "3LAzaBGjeurKTrtlHENg07nrU1FZo5aYa+fiFUNKCxfle4RBkpS04O+1wqdCkBuEALdtrMcHpw1H\n" +
                "tEcww/QyPlzB3OVpCBuCiiS0CPr6N67zYeMsnz13L/Ewqaodbz3lIpBwTTFKWEYA5VbgLUDhtkDy\n" +
                "EgQGDDAArerNW4UCSxWJ/wBLuYoi2TDUNrdMFx+TvLwrGeYEn8MVEEnUA2/JcCh1uWRVA2D04mzd\n" +
                "0UQlYPSFg31UxQMFw/BCD6saBb8QqpKkx2XjoNAAQRQoSQ0o9Rn6oBE4NkwQJyLQsiO4wK4QNNEn\n" +
                "qYUijEGhgPXVx7JJAjah94I1BOADQoNgIwkVjh0LtRzgxRuFOGoYFsCsFK9PhI9B9t+IT4FbnYeS\n" +
                "ST7xtFUEDmRWAWCb5HURkEE8UvJ2+Mw6YWtp0eG1Kb9GWCT1J10ggYVAyQA4OZoFm1glKUbCLcDB\n" +
                "7hDk8zkAIH5MerUpoKG+RAzqogOChlM9EQY90BXk9ohaGkO4EAYlFg7XxLwSyJbmbY7JAA5cQIO+\n" +
                "rMvKhgkyzi2p4E/4zIJBcnsGTYP4ARD/AMMG8lmFGDJZgFIoghgkDmhMBTxz1EE/ROPWApmgby0/\n" +
                "kbV8mAskSIRwEpQyg4Cff8y4jdc7B4LmKRVzkewRJrsgHvpDFYORGRGoxwpSP3/76OxHsd/pA3qp\n" +
                "+5gJpJkOIhodoX8bMI7ExCwJBxdQEBNE62CLJPbswfSoB8ACB/sg+8CXGmH8pRjnShJYcX9nCXFr\n" +
                "MIZggZmA/JY/MOyBM2DVw/AVLBguWuxB1jBBByBs5IA6pQYQCviCp+gEBQSqIPeIg1E7+tQwA0Jz\n" +
                "wv7DyK1Xv/JTQAIgod5ib2YrgIHkGBVgCiXDZPczDz+EKuMUDgRucraDEJRygtOUNZhjTz+5TT3D\n" +
                "X7YmGVs0D/p9yWgsbRs7MsvL/Ihw9xjhgeAVFAUAa++YASl3Q0/zMH0D/9oADAMBAAIAAwAAABBt\n" +
                "kKcfzlJCP0GjWj6/eloTSC/Q+eY5DMe7sXWqMp7xZ/wWpe2EWu3MA5qkcKxMDpNYPj8PNBaHswkB\n" +
                "ykyKU6BklSCzLXtsypCyG9/lwZ1Ea6sRUoSKDY3punYjUuyN3oy0+DKiVFNm/NxzjAfF/RGhjwug\n" +
                "pvI5PYVfGZ69EyHlYF3Hv38VD+Lm/j+Dxn0UlhRM+ks8Cy8KnEoENA4qH3qnwWC1snD3JHqgukit\n" +
                "aYovXuisPCAOYi0V0OvZ9ixLPJW2567wwsDQD//EACARAQEBAQEAAgMAAwAAAAAAAAEAESExEEFR\n" +
                "YXGx0fD/2gAIAQMBAT8QNYw4yNhMt3CxDW8bR4+2D34Mc249tNyM2/iEOQ55L+bFE5b9sQO26cuy\n" +
                "V5ajPjYeyN0tb5Jpa7mSfbdS1bapkdjvvsJ5Z9WvJHJjI37fshNG8yW7G5BdAy8OMHq17jN7euWP\n" +
                "cssWxkCRs+WMv7tdxnXdlbiDnw36lh286fDq3Dy6LMsIM7E3Y+gtjsQh1xEyxA4/uNUM9h1t4SnU\n" +
                "jxa9xws/FiN4QZdkZlp2+JZptuNnG2mbydgcgCwmFoeR3hbO5cjvtjnI5y3bswy9QnGJWCQKkhmZ\n" +
                "AP8AEsYGpZj+p7YfGm0b3ljl07ffkQGAROvf1LBpD+f7nealOtmftbAe5a+sbn6uuZDTX4Qg0vPL\n" +
                "x43bMYTxlDtm0H9imLP63/VrTycZMG3ae5c8n0EIYW/COQ0L+SNiokfbd6HY0nbf8TqO7BfVvM9n\n" +
                "k/H+Y1fyYD9pzY4/Ae2u2dhMjoepO7tjzOsw8sLPiNnZH3FGA+xwuM/TYsVDG63GR7eiWIHI31nB\n" +
                "20DJESGszFpiONYMbXX2W6EeWvue62wPgVDGyY7yeXgSWdg5p5AnwIFMl4wh52cQswsfxjCUyGZs\n" +
                "snvf+/7kvDbN+A+5ZZcw2A62OMG5+/CkPL/KUQilWyRKxnik9v8A/8QAJREBAAIBAgUFAQEAAAAA\n" +
                "AAAAAQARITFBUWFxgfCRobHB0eHx/9oACAECAQE/EDc+Y4pxl4FwcP8AY9Ux4axC3vAVViO5C3B+\n" +
                "Ri0o4ayykMxWkTC0a+4Kf886wBo4ip3fEsUzbnz/ACI0NcYaUdWc9SAXcwtEsFdYshvEnBURZYED\n" +
                "A5/KWuKhawY7RWhR1N4KI1WnhFumDTiF2+ZQ9NIKZx5ygDCAbqX2awGLiJprrBK0d52rnviUglij\n" +
                "TrLOSDB0PaXmkjK/PD9gAg+d4yrLlUQ67zEgfyIsW+msrpipA047wCmrz3gMiaRaA886QVkyeay7\n" +
                "aH8jjTrEii4lgZHCZCj8RFBw+zARpklKc35rMMxfnWMNu2IynbpmFErJBbjztK6YcHvAPbKahmCA\n" +
                "xk2f30iF24lkCWF08EZqs7QseL0/eUrAFb21XxLIHMmcecpVX162Q2GKeg7RtL0hyKjcK1+JTTKR\n" +
                "WrCDe2vmsspwjTVxKFMec4KeCBag9lXOOSBWh9/f9iwMjwT10iwMOGbi0GErxFvbaUkducREtlah\n" +
                "nzjA3zI7EZEdUbq3gZqswWjLsGz1ljUz5yhLRmJ5egMHoB5zmTTnZ4QIaeWjH5MO+ZXOlcd+cfbt\n" +
                "VfcECufnKW07+vneNQm0JldQBhLgxrEogFG3vAOssMM3q2Wjj6S4RKqHXbPTh2g1+oF474l7oeuP\n" +
                "5EIVl86TlzgQKFDPHWVriOX7UcAV/OX8jUgzNbM8ZuYBacc/LlDr5i4vaJiS7VHIEpn3EAYiaQq+\n" +
                "kolvMVR1xFaMhv8A6zO0c4xpKqJkhZRrDcOCF5PnWJobjKjUhWQVprU7iVoBlBd6lDVrLdSzzXMV\n" +
                "Wk1HTne8AVtHQ2r6TItdIHP+y8YGUzUzkh5d5g6Dn3+Zdlsw2HpEKbPeGoKILrWLbY95Wsk2K95S\n" +
                "q3sHmCXGzTieuYQPYaw6XvFuYMarDR1mXNYyFmQnj05x5kPZ+nRlIPrLDao0gNfOEV6KhVLZeXpx\n" +
                "lSexr6wrsffz0l6zK4gSzoxXHnDjGm9y41t8Pm8wwtxuBLAaSgHtMmlV7xWJ4QCSjG3H7lQuh+Y2\n" +
                "+by5QS9+nLEdqaVitcoTtQcOPWMb3fm0Al56R2D+Sx0G8mh2jWVkJhFItaixofOEyxrv9SgB83gy\n" +
                "mg+vO5yEdOsRyzWb8/kdmz7NYJXp/wCxDKdH7wwbjX73lggo9+8reRLyNMPOcANxTpsSphNC8esY\n" +
                "rbg/wgK6jFQzCZwGXw+5zi3df85RCHbzWLwz9hb7p227y3dZC1DfMaeyf//EACYQAQACAgEDBAMB\n" +
                "AQEAAAAAAAERIQAxQVFhcYGRofCxwdHhEPH/2gAIAQEAAT8QEi1LACSD8k+RwSVWCr1+/wAyYAox\n" +
                "4hTg9vFXkVEsWLpGfj+c5GDodjxPOOCjsNxL19B54841ALEiMlEB8nv7SWIKgsJuev30KQGCYSk/\n" +
                "uAK+dZt+UFW2+3fOKlKPKvxTzxgACdUPSvT94ABLcqPK/vPwGKjR2qNT99KyxnmQASQhz2cmFR3R\n" +
                "tjfQ5ev5yUgCI146vaunIVOAEJbBsi5emzqbqXsOyGtsev0ya0ARSkc69JvqxrBSqpgbiIntZr+u\n" +
                "EHBBdrPb/eMKYYSywpnZq2j7RzjLCV6VfHpxg2IE2gCR6TaeO+8ISWIIJJFiRcJrvzyCYFJK57W+\n" +
                "Ln7VUKjVx3I8/GUWUKkUrx/cdLIKMpJzXTX28qG+QWm6/Eerg6mZkDvbX5+Y6BRXYVMnXr8+3eGn\n" +
                "wdkO/vL3xl8BAhGueWPt4yw2Qurrbx56c3lwmghpnp/z/h6ZNmjh9OPTAygmS6Ivzd+peKTVAoTz\n" +
                "PPf84qLRT/D2/wDcKEIdEy9e/T7eNiTRQyPT37V4xiESMCXLOvy/jnDEgAT1Jd67RfWMlXI5UbuW\n" +
                "ux+8WwhJevX211xgEkEywHSN8e3zSpCCxtua8yeTvgiqIDbmNjzQTzvvkLmkD5Jvv6PnAEgkJh4m\n" +
                "Vx29GAyAsweaSa9L8NOBO6ipiNB40vfSsJoQiJeKSPM16d8kadEtxqf0+/Oa6SAsBHu8uCQFMAki\n" +
                "UdX3jn+hGBQTQiEvsx6+ggSGgI1N9Pn8GBkKYSeI+Ldf5jUNcEuFl99uOIZDbRi9vf3jtkkwodXV\n" +
                "n93/ADATMItWZ176/wAySFnFkti+/HbT4IGOwiVj79F+zYgJKm0SLitJvr7rWgGptd8QEbwEjJDx\n" +
                "0vsPS51iBJBcsR1qPQ9jfLHaVMX5+/zFMQsslb979MQYlaEeh9/uSDfCGtV7pNYBLIGoa7+YTXvk\n" +
                "rZHgf194zoZwdJqPwe2QcMWGRUxUR/uPUkRhHjc/D79oxCKEoifYnpfqdcMxQWYOt/G/dwUWJ63b\n" +
                "BE/fPOApBghgd+F9fbtOOYIxkCYOk+tdmOuMkikG8PPOoJOfzlChE2TbEX789eIhX6qAMEj+x+Ou\n" +
                "LFk1AkEbH9d/OTRJbS0RZXfnu4Op0sB1krQPq+MlulQmTmUupmOvFOR4yHfLIukJ+MH1MUEG+njt\n" +
                "XG8jYS2YdNMNM8dTNMmQCiNgDLek75pqQaKIjiBx1dBxLMx+mJyCZBpsSFtO9+qdcKYFaFccFdJP\n" +
                "7kkDYgC9zXf23GSIPCCBBSB6hfeJOEQLEoH2E7p5POMJjaHaeUc7fPbKLKYBXAuo1+SGiYxQipKh\n" +
                "EHMeQ9sZgXlBQskljmJ1F9MgIEyFjGqa7Tqj1cIhDIWNqvr0euQEioAkh3t6XGOSewnno/8AG2wC\n" +
                "Rp7mtxkGUBdRqLp5v035yaVFiISRTCdeb6PqGIJMs1yHpEnie0hrAVG48+t3vnzFiQIypZIjC9C/\n" +
                "z2YI2ttaYWfsnjJIKS0sq6Pzi1JSlRZo+D1MIhcJyIivw+2ACE6Amb3VjD9vJXMkiiyphTo+94QI\n" +
                "hhIsN3PpWuuC7GFQxt1qwy4kSLSoR+L8rwyS8hdgg4E2DtffCw7GyfcDtgJV1GMxNAMOYqZ0b9Ix\n" +
                "OKyiMIzUvS8DohNJjqeyfvBomqtiNa9fftkYzgKbRq48GP8ArIqLYQtNeHdmGQuLE+zKLJqNuGjZ\n" +
                "QU9ap8nt0xkpL5gcy88fGuMgbRLFqph1032xIYShlDF9db/PTEAnAZ8j8R3jrGA3zUHQ7iuJj/xX\n" +
                "FgHrUF7y8wx+InJGCMQUhMzydXv8YhYmoFWut1x66rA0CREOp3NNPvigoRMDHRHmDlzOUXKwhBoa\n" +
                "n589OZkJC+Lq9pn/AJKFK0JJng9Q+uAiCwgd0c+IfTxkJqQl8ySZDo17+wORcCEizvm7n0wxqGIS\n" +
                "eQjWt8c+YYImCGNwbl94/WECoyC60J/d3jnUsUDjnvOADJmSIMMafP28ACUEWGJbLmOELw4uBACQ\n" +
                "Y/8AX9yUrAVMh59B/wBTJrASerBRMa3PxvKJ9sAF/Mp64YVI8BNwNVkkNecIdIVOdtvZrAtUIoLI\n" +
                "38T85JyFiCPT7vLFYClAnQNe3bKuVCC+khzP5wFPYZBIlrjYfTBcc8sUC+vJfT3Yt2xInV8/GuuL\n" +
                "RLxdFiCcw78OaByygAOtRWw1VYu544BUqJ4HEBDhChml44n8XvCCLVUlN8denzjkCBCWzIIv0Iem\n" +
                "SSlgmUJVt/XpkQUgaA6eDXv/ABS0EB3G/wDPbbHDUAZ3DP8ATtcdMWoKANJFifD0Y6mBxyUVO89+\n" +
                "Z4cjYUsiJTkl9IOp65H2dINB8++ntP8Awk8BQ7HNt9fb0wcEIpFW/vvOWaVUm01fzjMQSw2VdPKk\n" +
                "Ht7vAmeAQ01x2ieecsCRMfQtDzxydZxgASck2R1viX9xkEhmQLI8eWKnICmhJ3MG+Hp785eBASjl\n" +
                "+O38gwYIQgXmOjr/AHsYmiFYmInmfeefEY8hgy3Sbl1CzPrcY+G8pAgK2GnV8YkqVEi/K9p+MjCs\n" +
                "EvS57+mSDSEr2lJdcfZcoLBKC7Zd3s35rBgATCmqI1fPbeHwUIblV3WPt4BnQiEJBt9sGmwDYEmi\n" +
                "TtPX2yYPEDPQu4lvtzhpDoJkJ5M9Z1h8TQVLTPOqfPXKfoakKAzomJ83hvRiKEVx1h6Ax0yUJAki\n" +
                "tAOejvpGDYCSSJeV9jxLnUgRTUsa++jiGIDYunya/wA4cCkSUhWWZn9p8YHI2GuYaeDQ/GKl8kIG\n" +
                "LJuekJ2rpiAhb2MDE/gxyxkSWgo56bPP5VlsAnX+J/yFIVocyJ19f7hTQSLJccHn8HfHsGTSZobj\n" +
                "4J4mcYiIKoWeWzVPTRWMiDQZFC8HQv7qrpwkEWf58VgRBVAl94XuLfXxiBpQpI4sT2/PnE1cbehr\n" +
                "T4+J5wOpJXS39+HwUAhLA7GBXf7O8JGYzKSrMF9+sfjWMwujINr7EevnFbOgWbP1xjqpJgLjvHTJ\n" +
                "hEotBndT1+zhcIxAxBQb7/8AuEFoYGGTYdGdT4jIQoU1ZNt8z058YORIlWUi0x621TicE05EXacQ\n" +
                "c91bxGhOgLggjk7fYxJDJhaBLF+rGSmgWkHWydlx/rIGhJCu0ZPw3xD2g5pEqBIh8RfnG1IhJKI8\n" +
                "kgnwmsjZ0IC12mSPjHEhBOSo864+fLkAkMKLxJSdoPk7YhugtnR44GvniExDBSHggPvXXXAiZKJF\n" +
                "Zh3jx7+riCYCRBWSLjtG+uElVxccX19Y9OcZBEAq1Hp2jyl7vgq2TN3+d/P/ACVcFnOx+PvY1Aki\n" +
                "BvZfXiu+ABAKQQJqFk6R898ZiwEBVu5jovrhhMljpmNG+m+52MghUyKyBovzE/jlZElQDqDj1jp8\n" +
                "5TCRAbGJ/A+cEVKAk0V7+X2ntioJKjha9Ljn/wBwrtFy4lT8zz351g8RLqBo+n4xTKAIKTdkxMIV\n" +
                "gkFkLRL042yVRgtYiBUpv5iP/MjoBJ4VE1731wGsCaEhrXj5yi0BAOGfvk74QhdYAcURNa8uMFgl\n" +
                "aWw1xJcRJ64odQSwkBCvU7VkpGxbIpHVfR8kPGlbRbR9TjKQghGSXfTtWt3GXhSRCV3PvWJABlsp\n" +
                "Vo+Y9PTFMV28OAGhh6OI6YF11W+JnpIVGSCEsAVLP9WLrEgJOoTL14/lGBOzRGyLXs++ZRUkKhTg\n" +
                "vutVE4syUJovhTXE/jrkcQQkgJPs3xGBpQR11vxz/cPlSK1ErTdRc+DvklZIAOjAb+7/AOBFCFYi\n" +
                "Xhe3POTJEGlafG6k/OJYSgmWNwHsn/rDaKJQia6+hhNUFDO2D249uzkioiWoCy+t6nJw65xKz1Iw\n" +
                "gcKBEpdnH+9sYQS0SRvx0+84LJWSViYme/jx1wCEgTemv47rJRPRsC9VXXX0xuwBYQACw5inrvnI\n" +
                "xbYQpAsd59OawhJkzAqaP12vvOC9hKRpqPZv9ZBkGsTFseskdnAmINgSggdljXj2QJcAFwxcXZqe\n" +
                "TB6tB4CsMbbt42MZHdMToC4JeDjiXpbkyIOyerHXxUdsYQmegCJWzXxjLNChMgfgsmiJJ7qBD0Bq\n" +
                "EpvtPTEcolRDmAuYiDpaYDuKADqEBy+MYjCOyKmI2S/GHCpCLAAzG+h2nZqsUlhXB669Gu3Wccuq\n" +
                "JGF0z22MvS8bSshAhZo/ixz+AbWdRpPP31rGNozElUiydjntiKsWQxBBt41vs5MqdIG1afA/GF8u\n" +
                "dVvz9L/5zGIrVxv3/GNJzi6H37HGFRKSwRXjv+OuPGASXl07evMYrIMrllJ/fX+YAkg9Jff4/wAt\n" +
                "yXcJuTEXM+e36xraHk0bv5Oc3ctTom4Xbc1WIgcNAB2/z19MogIZRw8jPnWJhS303jfrGu+HXGjF\n" +
                "VX2mDKD2zOZdlXO364QAoBIh5t+675GhBsA30fjuc4QZgkgoYDXcHb0xuGSaagJr1HzM44ojqKZj\n" +
                "+jzzOS38KyY5J1Kt7yVvAUkxE1uoNHLM4tkoMCBuiCbISOxwtHDTFcsTv2i3nBwSKsSIgGFjipnn\n" +
                "Wa31kblNONlXtweUEMBDcLph08M3GHhlMkDmilFc63U5M1S1hgo3bPp5nNs6TBMC096m39pihEUk\n" +
                "3riuWPjLMhSHWB3ov584gQBCysiv5rrhuQqSTc78ebmZJyc0CSAh0310sdt6hiUKrKp611/PjFyE\n" +
                "1k402dCYPHeHIRGFiImP7c9f+GGQFsoDtfEMYmiiJMETRU/YnCSUJk8gnI0BAsurjjp27YTklqKk\n" +
                "9XT8emVCNpHQvZ31jWxUQcKia/mRJhRXPWfw5KMkSuQ/3iukdZwK0tADyvSe/pxgTSStm1pr0dd8\n" +
                "upcoEJQkv49cUgljMbY9xI7mSNCORVJCp/Z9wIPFEJLZfhLWfTImyCosF6Z41/uJaEVypuG/Ncdg\n" +
                "x6hRuNEcnx796UyTc0qk76X1XDQCQCoIok0744xJECMKjjfTj185RzwICevd36l6oNsFBVexHY9V\n" +
                "3GJj8YwaCk01sJo4bW+LiF4XHn064xuVK0KE8a8/gkBJA30wzp7P5yFADBExK6Na3+8gnAvZrjnv\n" +
                "q9YQGGIQjNH9mvoIVDYDlxt1vfxOMItSmslnvLrtiS8wCWUOxyH2sYLcAIVYduGo94sOK5kcFtdo\n" +
                "Tn+kqkyqRKQ88bB1p3yS+wn05PXzv/gxJE2aOnP3eM0DLjYzqa9YyWKBifKSP5+cMwkATHWt1ufj\n" +
                "Hp1ypxVQndjjT1yNLBU6cR9/ZgoUWpTa/g3z6RlRBEKpXRvrx8GIpQmYXO3X33yUCwEh5nqx9pys\n" +
                "RJQcH0L/AHjECtaO+SK8ZVxkiaYhOlku1PTDRaRGJlNz5/O9YeAxDWPA7dYyJBhg2dNTn7oc5xcJ\n" +
                "RtDvUi5pcv7XAt+wWeCByRnlAjvtHvOo7YYUzhCRlc0bfPXHCbSSdMIapvmO+AilgvFp2wTEioeu\n" +
                "EQYLxoVBLmZHWucLQJDqImR3fF/GIYLYLkQnXQ9R5JxxJ7LyzBNdSIq3vlV/7PgjcEJm6cAFo2hO\n" +
                "5jafT2ibMbPd+GOuARx4V09vHfyAlZLW2RufHF+cVTvuJs9FcHeecGEpiqeN953xfnFEQJfKBF31\n" +
                "fTFL8CWISf7+e84wihHJPe+br/zIGCNYanu/Y/4RGJJUTFIT8fW0wiIgcFRHeJ9t1l0bKAcON63P\n" +
                "njJAGXgVw750v0MXI5KiWX7/AOGRIAvIzPmd/wC+YEDxJb0nblkdXvFEQVbqWG/vSMgQEMSR0/v3\n" +
                "jBIBEizzGr9N55EiNhx+J7kuuRCJFJR33WsdxB1VNln9tXGNgCUfAKMmIXRLmZ35+rwR9CryrqHo\n" +
                "xPPriWDckQuB5eZC8UgwGwvVbbu+u++DTElKE0X9/GE3JtZSWub0PPaLk3JBAuymCTU60JeKUjMy\n" +
                "QViSXNaygh6kqDrf4rrSOTCjAsEM6t5Oh4xw4w3c0+mtXMYRsDrAzbP8sjhvGJJJNXAKRZSJTCuc\n" +
                "5N5YrYq32aJWGJBMc6nW4D/0ZGRAdEEI1xrr/gpKyl9QbR278zhAElRLKMLue/yehCIQEAdiUK0F\n" +
                "5QgJeaIcHXXtlW6TlKWD832x6QSSh+B3ntOBw4oFT9j/AMlJFQITXEfjFrQE2264Ok664RiJWWZu\n" +
                "Z++PGSQCWEpHS9F/lxkRRAw+fnnGVTAgVKuQ5hxdAKtUg7ri/dMQQLUGtmvfLAqlR6vxgJxtpEwy\n" +
                "fnAVpsgLmGvN5MJmCIu/Hxz7YSucIRUh25+N1hYSOQFVzVXx/MEWGIS6NASevjvGTQhMKJHQ8kGq\n" +
                "5xnVh9SSpwrfGuuN/SQVsxu9Ry3hh0gaYnJqOb88RgKJpiobJU8e2QAHYIadSmjnEE61BArBcbqJ\n" +
                "bs1VPaRO8WEsS13HORwoQLJEwJEojpflhxRADSVfiEsxuDlrYmAamIqNdMfIRhkhCAycaTm8vTZz\n" +
                "ZAvvIemIgo7JpZJXosOL30wCpAJMNO9Ts9eLyTVgdm+F+3p9ENYESqkrD6V8+WvBDwJBv9e51nII\n" +
                "wSRTUWh5+6jIYjwkpZ8+S4v3cUKZTMBqz3T7EY+pDIirq+7/AODQSKMKHji/jpgsiJEifYp9Y7YL\n" +
                "RU03zT6fGRZyJBRcW/OClMqoGOiN11PfJLWEgMWi3jyQ9OMlJ5Ecr552e/OEVW2gq3RkURQgX769\n" +
                "pypMI6/G/u+xkNiEtEUWTvpX2cQKSV3CxdRp+esCCTCMHkPSz0jpk5EiYhRLL57fORCmmF4RB2z/\n" +
                "AO3isdBWpi1LnXxHgGCdQpKGButfysBCI20pmFk1KMTF7wzCgYkYeA3qXjJpVMFksLoZlI86FewG\n" +
                "IaTV7L6Y/wASzFKJY81c2avIyTcQDgWS+L5TWsUqYJDaAF5Ya6dHImCisQS1hJeGfUxFKMILYgdy\n" +
                "wS/SY5cqiLYEFDlUNUYMtPCkbWpgnvessmjDNFxAebSfRxJFYCaUUie0f31g4EAQ6vTQGumUvJCW\n" +
                "FVYb7jhDY5RtNPEw37Ygbumg869PnzkkqwyBQh6+PW80aIJBCe/3t3xA22ZcIP75/wCSC8TYm+Dn\n" +
                "p+3JSYGERDInzE716YCyy0VpG57U4QoCgZDi48X+PXIYgLVN8II8X3K7MBlUgFw106X16ZDAoS8k\n" +
                "l+fXr6qI4AVwxfp98CQLCbRaQ9tYIChMSdBPP3pgBIkGVArzHEzz2OmCGLGuYVAe+SwaSlFIJh5N\n" +
                "dnBwwSF3F+tt1FGJxFdIwXmGNEtOldEYiQUbBA0zuOsdIBA5SCQgB5mN01jdFrxaCwUrUTLERxGE\n" +
                "VBULKQRK3danGbVgglCKl3maiMmZS8NDUJLwT+YsybzvSKgg3GouMh1Cq2SwREPPS+pkWyKJFImm\n" +
                "GCX360SPMCAepL3XSfXJXAq0UD0GlefSIKiIl7gSsrbvcuBSyMrOjJiZuWep2zRQYJkRLEdHn3nI\n" +
                "UFrJTw/MVzGIGUTFEK9Hv+MUUz4RRrffnt4hwQICsMihqdcHr2xCEiSLFEIN+d9m9YKUpELmUNMo\n" +
                "WxJxv2WAKMBlVUVx1/nKUYTO9X7ef+JWKhIU1sj1983WyowNEvvi8gyszsd6mgL8fK46SEooDP59\n" +
                "u2QfgBQiySV7fOQJThImquJHs+t9DGLOm7dpL98djBJgwyhDUMdpciCqbVF31puO0YYTSsI0dp6Y\n" +
                "ohQBlsQSaXxPs9cEjKlszexTp8HbHEHAoUigPRGejGDTA5QwaY3MqTu+l48IaElLVS9ryBxqKrF4\n" +
                "TC9ztOEjdts6ow2tP8ySCEWQmoi6PSMVA5uwRC0JDRV3ijgpEu/iLaBNOKRAuDZnrCD0Tqy5OpqJ\n" +
                "HiMjfRBWBYwBmIVEu4enCnmrsMAgVt9yCnfjmXBGyymktGOqWXd4ZYZTmSJJKTSt8ze8d9nH3KmG\n" +
                "WpiIyeklMKiTYJE2dOcSRaj3BFRfK9pITeMxObQEsiva99L9c4S8dETcdS09XxglAgpkSgk3Zrf5\n" +
                "3ixvBlMxAqdZmfJPfCsLSx2MR8/i8gEE2PSGo668xxWQTK2SZYjX4ftYMchljr9fn/gmAsgQUsxx\n" +
                "64qSAaGKu+8/Zy/KqsA68dvJhGSnlotEVcf7l7BQNhFV6sPr2xbUmVKQM+0j5X1y4PgsJmIufE15\n" +
                "yCSlE6GyPwYCirUCxrv6Q+MgOA7nV+/+dMXWl74/WNJKErXKT8H+TgIRTttkWe4ntGsLoHqJnX1Y\n" +
                "fY3kaGUSQCmzckzHo4S+ySlOpEK1uPzeyNxMjkkHe9c6wWQBdbEMBk1siecXkOBHblF46u4HGIUg\n" +
                "BZJEtnnv5ctvPijC9dplceMCGZlIZIikEGqTU3iGWsykJYFmizo2Tgo6oUCbIuAmfZZalAitJEbh\n" +
                "Goh9hgfLDdSx6IhxuPZBACDMojTrz5r0nYMQFWyp2Xz6YCdhoIx/Vs6nOoglhS0SgavntU9JkgpC\n" +
                "FwgzJ/vDGFwJKJ8ib0d+MKSKRKovZ10cS27ywMkrC2kTN8XHpjcSM2t2n6/GIUKCEjrIz517+cie\n" +
                "YywNszXj7H/EEEQwCAMQVPD6e+VIoQQG4ifkmOp6tJosQo0Qff0yRqB0Uf1v5xymQpaUOevf23iE\n" +
                "gBaHJrwZ1/MTIZsWQmn109sRSB22P5Y+r4cJBHvpaPyfOaIYBYO+4+/GTYiMhPEX+/bzl9wCNAwG\n" +
                "u7/mEZylh0zQdTj4wTDLZggUQ66z0I5wUQBbViSzbEyes7jENwBkSuldzf4ly1MTsCiDjjvc5AHQ\n" +
                "AaWLppYN1XeMPLE7s4JQ9NeHjSAxI3JCdRC9NTjORqGx25l6Phe2UpJKA4Dq40bkmZpxclhmIBSa\n" +
                "CnvWsWDDAqCi8QwPXTmVDPoEJUQEIE6NqdsB60kvgalsHV1eHV7CAEywSzaa8YaBAVcAVQjZXX2y\n" +
                "NgiuxfBWmd9TrWIEwpLfUszzBZzFd8iIZUJ0TanSl7/q0aqUWrYfj7ZjEUg0HSl+HnnFJGDZOl1U\n" +
                "9I1iB1tBvmKxmhkKKO/4+xilQkMt8/r/AMmSSFJkSxMVv9uCh+zQ3c/yO3eciKUhMyamifvrjhmp\n" +
                "mbuVz0uMloagQyukHpw9OMLcASBRNxEPY+6yFgqrMFs/BfkrF1Aq1pmeObjp64SwCJbMrR7cvfte\n" +
                "hLLjVxl5IWhJwiWKiG7IXEvPtnD9GTF0yjE2q73HraOd9ZCiFIrWioIu6x6VSiRLQ3CGG3FAmxur\n" +
                "3BXY9Ea2uhATAG4oL6zOCwbAGIQBXILxXwmGoFibAizejhvjA+ip5MwdPNX0nmO5YFmwCS0FSZjR\n" +
                "juaEccASUlEXE7WmQPRq7EJW9KATvmUT4dcdQPVfrVQr3OahJpNVZxxLVYh+6QSku9ZkGuE8RQKZ\n" +
                "oOfWkRgIqA57QpIBQKExJrXxpImYojMGkQ5eh4JQNvf2OK3iTSL4UE8nVsa5osCq0dVs/JBxbu8B\n" +
                "MRSBbR56+/rjbuSzMENduNH6yJAHAjTWvI+g5CUBCBqaBKvj7YUltJTY8fN+mDIbXqO58z/waqmB\n" +
                "CAPteMUhQxA9G6+OdrzNS0iYmUgfTb174SCJRlsRHq1v/cpYRHZNfFbuXFzVaYpDM/7lihFwgd/P\n" +
                "2Y75QYwcQAKimpuAyBmARJ1akF8Dt0wIzYRAYAIg1AdX1m6mCxgeB2QgrDuYnIQpKU1nYerFJnDV\n" +
                "Jg/NDHXkJG5EufSCvbgj/W8VQNljz15a98OBNLMxBJV7hMjC5spYaccH2MkkZFZFwrlZWojkycxd\n" +
                "StEFCJB6VG8jVEzFTEMymFI4RqVWRc7LwoFxMrPfwZEBCpDJTPREdSTtkRQXakkzfcPZq4wyddJE\n" +
                "MEdjx03j+VdAIrIQ5hXXzEfjaSEQIesPqc7BIi6lmVECwZRZJMQSdYxBgD1EZmFlFqNqwSYiDEDU\n" +
                "NUPsMLQ1aElNlWKZqAKBrwiwAcEbRAlLCDaCDyV6GLZxKYeYJbN71PRyLBGwrgLaTb/XpQSFUnS9\n" +
                "Bebup9d4AmUWhKSfz5y5owBi9r+Tpr1/474Us2HHXtU+Mh2mFTsQxvn+9c2slS7Nbl+8xgFJYBBK\n" +
                "YT/ekVrmk3GBNbOPvbKmHoN1Ne9elRgs+WlywJiLTUksRtzcKBqEJdcAVDCgFyFEyoEJAZQoMwd2\n" +
                "LXIfMIYDCgIBwFSUelJQhRUqy4WNMiuhWiYlBWmICAGRsQyWBU8nqQNgu2lmQvHz09O+GDESLwYR\n" +
                "Dpk+3gpKExDKDJGvC+zki876B6E9o84VdktMwpV94zbqRMsBPT/zWIStUxYbQn/S8LGA4E8iwsfe\n" +
                "B2LzKz5B1o7426kSDBo0uDmLjDPZaAoLJe8kujquSexgHAIt0ftnI0xMwpKT3evGKyt3gBFwZ0WF\n" +
                "FIkiXOQYCiAgEoUkOBmw/UWpWsS0RExhx9CUlyDBRDkEBe4I4JIZksv264uSYsjuGGSFYgAMOGVs\n" +
                "nnUcGdxJ34vHyhGzZ26TFnQw+K0a5/fTuK9cZjIaNBMj20V3d2hx9rOkfoT/AI8jICYTKER5nX0y\n" +
                "YXQIJoy3X/vvjh4FGiJ+L/GOASpzTkzQB477yhVRpiZl0NMdY1iy8yCpIGGEpmkxY4SJE0gIjmtd\n" +
                "9YnM1NExMRVoBVt0GMIgAzQUmnA0T1BdsqJsXEjbL4hMfAmUxyK6DpQdDIC/I6CCVv7NZhwOlBGd\n" +
                "0ouzqumUqQq4lEzNzj1wYXMPOt/q8OidSQUUzsF6dcaHKM2yRmdkTvpFY/v1CuNabquD5xEpiJgi\n" +
                "WW0aTXf0KpAVIKyk9ZL+zhGfpAPPEN+s5VygSRZUZ7TZu7guLN3KkiFhLMEvWawj0ELIYSgQbL3y\n" +
                "zBio2AewE9NPtLgQMHRy8Pz77xSExQXIj5vrSMKqCloZIM3DDhEBYcEpGgREqSZxJzANFUiRuQHO\n" +
                "NumRpF53ElUqXOXa/q3C7IQLCkQZJwBtFpcQal3w4eMAEwWkhqUzPtGSFmkQ6Lu8lGTQpEf+zP8A\n" +
                "mRFQkY5uZ5mfjv8A8EY5oukLfXR84bgnBq9X28RDqSlHgoMrNEStAayQd00E04wSWD1Es5Aq8Agr\n" +
                "68II/eIihNdevB5Z6tzlqnAZAQsMd9AtErGHduAPDA02MjeRCEdaz2JhJpZdrW2cNc8kRdKGZVLP\n" +
                "VxMxSWMsIiK86wNTI7sY6Rba5lM+6HtjBERIW2u8bzkBw5NcmQ6Kymcicta5+znFaJoDYX7x+YwK\n" +
                "8HTVNxV/+Z4dpBh453Eep1xtCFCNER4mSeJni262UlDknRZx33wuZOEQlAOrRwdpxmygkJSQN7Rf\n" +
                "J2ct2GrDQDjjXrPGAmAQMQluBt1vmMmUAEqBMKt9dHK7yBRlTTCzfShDr7GPBrQ6JHzi+zeUgHS2\n" +
                "Kktq0vfmGdm4FLUsSpASCAbRWoCgXhmQpDhSwGVAptKKJRxA6cw46/swfQURdyBCQfVZmGGbUR3v\n" +
                "qGjXVcQb8AMqGyeGXXTBvuRjQ/dhMUYxEwDXh/MRr/k0KJQ0Jrt9N4EQ/QVKylQtXhNYGHBFBYVh\n" +
                "M8G6IIuXg1SNBLWN/wC44EIJ7CQUxMwfbxinUgtGdNrVyqQSv6VMUz6ug9AGPlPUgCSC2KYlxjbX\n" +
                "686HTGmVJ1MTK2CZQKWQ32kdL0Hph8LSWWWROCjl0KymCDiiASDh56gESkVIBHkxe09QzgoQJxGv\n" +
                "JrIlAIieIQ32n375DApEoXr9fjDYzQO96rcJrFxFtpRNTRPX7Z2ziPRvu7nXPrjkozYJfBtbxqEd\n" +
                "FgLEwM8z49JcqRYVJ52rYdK3IOQckSBEjMek8fzFYI2p2bg9O9zHfLWbRFfsdQdaOuHsbVCTHwuu\n" +
                "/HK0XWgC0PFsHQXtgCkGXEhd3UxdVkQ/KipiRiM1p2GmZMwshQAA51FSQoZ/4ChtHo1V0Bks2Yjx\n" +
                "Uz5lN9FOFBUtz0enaEjiMggKzlSIBzDbv3MboSmF6DW50xEecvLGKRi0x2KXqrOsZUq+Hf8Av/kD\n" +
                "wmhu6Ceb8/jDc1FMzQevopEHFGiEcivwBXM9MjNlEhs6391XR8tMiz7Y2hsTEvoZTKpTt33LTBMz\n" +
                "hMOTNACallddIU0DsY0SH9gsETBkdlMVOY0geeTuBk21fzh7n1cXkOtUcKY0sIq2bxfXSiA5fQjl\n" +
                "5MFUO4mUY6S++EXCmStALcE6PHnSIjlCrrfTFUhokqSxz7e/nHBZoilu+vTXeuMPxQAlIPf1xyDC\n" +
                "9hoFN+vmsEgShh4Xr2/3FmOHVwzf6nhxZAVAxrgk1KV6PhHbhcho49XGmy5hab+YMqHHaiR2xq7i\n" +
                "lTxiKiCgBVTeSUaBKhiPSRk4Q5hUSwc04QvINxCaOBmJiEj/AHCgjwkesklIJlrJ4AkQBxI011ks\n" +
                "jK1NW0MPWiVlxKMmoBsHRJD+hHCRQ2vGvJk8hsU9IeSIaH1ceNnmLETPD7CziSmwjmDuZ1Lo7XeJ\n" +
                "1NigSAdyH6/8n0FjogWcgFdlkrXUVnehKSvWSrOD9qyJxeux6Liz07Ng04wL2QqhDlM0bQSwMWXq\n" +
                "oVaUMC54qw5tlMB7o7xP6lQmTPGITBC7oNbBNcmgxIcowCOahr/2xR7bynIg6mJ3FayJtQABQiVu\n" +
                "YOsPnBGzsimcGcTIERzBOSAtIqpuzWum/TC8OuAwTsRjzcjePt+dmVwh0TJgocSJNFIkKImzmeRO\n" +
                "0URVgRTFxqqjX8x6IlliZOYvv+slU4UVPGtRVZVo7lSSTju/5eDcuDoxBf3XbBiiRBRBfgmY8hxh\n" +
                "bYOhAQva4I8echVpwpBEvYuPLkK/YVOXCYYdi0EMT9CnIVAZlLC2l0ceLCRGi0e9d8iD9FMygVrL\n" +
                "I1kYb5jnT0pXUcNiduYh5AUDYRDJgWiIS4omstEkLSuy71REpW9dlQORvWar6FsJCOsGkG6gmZ2I\n" +
                "nkTk2+xMYFgur5j3xW21pcT0eJ7HODcjCuVPifh0/wCLIg/oiD6HwdMd4DhLgPyYBzMZULCKTqGV\n" +
                "bNZGjQUnlZ9Mb8g46Upypy3iy7FaBCDox8uAzLt0nQmyR7coRP8AEdJmFHPEsOB8sNstL1Yq6wD5\n" +
                "D5jgTtedy2vGLH6UAlF6r+tGGWwpuE+EZe+LZXKEGB8Y4D7guvvYx8IIKatM+8a1vri1oNGxdXYs\n" +
                "ccCrPiAJaCvTAhtsyFwpj0GCiSx9/lYskZAOIVeunfeV6KTSQytcsF8pWkGmvRkgI6CxaZTnGCzu\n" +
                "ut9hgczDRKae5SKCADEKiJovlvEKUeIwEGn1n4MPoNOYbIbSzbO8k4d5LBMqDhV6lwIkUWmpZUlV\n" +
                "iL73iPzgqSqYbu3gfGKjQIZV0x6GA1zrmAEs6IhpgYG8p1dLIVOMYigcWFT2R7YDsA9RjxK+Mqi4\n" +
                "SW2236/B/wA//9k="
//        Base64.getDecoder().decode(base64)
        Base64.getMimeDecoder().decode(base64)
    }
}